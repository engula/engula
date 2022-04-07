// Copyright 2022 The Engula Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::{
    cmp::Ordering,
    collections::{btree_map::Range, BTreeMap, HashMap, VecDeque},
    io::ErrorKind,
    iter::Peekable,
};

use tracing::warn;

use super::version::StreamVersion;
use crate::{log::manager::ReleaseReferringLogFile, Entry, IoKindResult, Result, Sequence};

#[derive(Clone)]
pub enum TxnContext {
    Write {
        segment_epoch: u32,
        first_index: u32,
        acked_seq: Sequence,
        prev_acked_seq: Sequence,
        entries: Vec<Entry>,
    },
    Sealed {
        segment_epoch: u32,
        writer_epoch: u32,
        prev_epoch: Option<u32>,
    },
}

pub(crate) struct PartialStream<R> {
    stream_id: u64,
    version: StreamVersion,

    /// segment epoch => writer epoch.
    sealed: HashMap<u32, u32>,

    /// Log file number => mem table, there ensure that entries in
    /// stabled_tables are don't overlapped, and no empty table exists.
    stabled_tables: HashMap<u64, BTreeMap<Sequence, Entry>>,
    active_table: Option<(u64, BTreeMap<Sequence, Entry>)>,

    /// All previous entries (exclusive) are not accessable.
    initial_seq: Sequence,
    /// All previous entries (inclusive) are acked.
    acked_seq: Sequence,

    log_file_releaser: R,
}

impl<R> PartialStream<R>
where
    R: ReleaseReferringLogFile,
{
    pub fn new(version: StreamVersion, log_file_releaser: R) -> Self {
        let stream_id = version.stream_id;
        let stream_meta = &version.stream_meta;
        let acked_seq = stream_meta.acked_seq.into();
        let initial_seq = stream_meta.initial_seq.into();
        let sealed = stream_meta
            .replicas
            .iter()
            .filter(|r| r.promised_epoch.is_some())
            .map(|r| (r.epoch, r.promised_epoch.unwrap()))
            .collect::<HashMap<_, _>>();

        PartialStream {
            stream_id,
            version,

            sealed,
            stabled_tables: HashMap::new(),
            active_table: None,

            acked_seq,
            initial_seq,
            log_file_releaser,
        }
    }

    #[inline(always)]
    pub fn acked_seq(&self) -> Sequence {
        self.acked_seq
    }

    #[inline(always)]
    pub fn sealed_epochs(&self) -> HashMap<u32, u32> {
        self.sealed.clone()
    }

    pub fn write(
        &mut self,
        writer_epoch: u32,
        segment_epoch: u32,
        acked_seq: Sequence,
        first_index: u32,
        entries: Vec<Entry>,
    ) -> IoKindResult<Option<TxnContext>> {
        self.refresh_versions();
        self.reject_staled(segment_epoch, writer_epoch)?;

        if entries.is_empty() && self.acked_seq >= acked_seq {
            return Ok(None);
        }

        let prev_acked_seq = self.acked_seq;
        self.acked_seq = self.acked_seq.max(acked_seq);
        Ok(Some(TxnContext::Write {
            segment_epoch,
            first_index,
            acked_seq,
            prev_acked_seq,
            entries,
        }))
    }

    pub fn seal(
        &mut self,
        segment_epoch: u32,
        writer_epoch: u32,
    ) -> IoKindResult<Option<TxnContext>> {
        self.refresh_versions();
        self.reject_staled(segment_epoch, writer_epoch)?;

        let prev_epoch = self.sealed.get(&segment_epoch).cloned();
        if prev_epoch.map(|e| e == writer_epoch).unwrap_or_default() {
            Ok(None)
        } else {
            self.sealed.insert(segment_epoch, writer_epoch);
            Ok(Some(TxnContext::Sealed {
                segment_epoch,
                writer_epoch,
                prev_epoch,
            }))
        }
    }

    pub fn commit(&mut self, log_number: u64, txn: TxnContext) {
        match txn {
            TxnContext::Write {
                segment_epoch,
                first_index,
                acked_seq,
                entries,
                ..
            } => {
                self.commit_write_txn(log_number, segment_epoch, first_index, entries);
                if self.acked_seq < acked_seq {
                    self.acked_seq = acked_seq;
                }
            }
            TxnContext::Sealed {
                segment_epoch,
                writer_epoch,
                ..
            } => {
                if !self
                    .sealed
                    .get(&segment_epoch)
                    .map(|e| writer_epoch < *e)
                    .unwrap_or_default()
                {
                    self.sealed.insert(segment_epoch, writer_epoch);
                }
            }
        }
    }

    pub fn rollback(&mut self, txn: TxnContext) {
        match txn {
            TxnContext::Sealed {
                segment_epoch,
                prev_epoch,
                ..
            } => {
                if let Some(prev_epoch) = prev_epoch {
                    self.sealed.insert(segment_epoch, prev_epoch);
                } else {
                    self.sealed.remove(&segment_epoch);
                }
            }
            TxnContext::Write { prev_acked_seq, .. } => {
                self.acked_seq = prev_acked_seq;
            }
        }
    }

    pub fn acked_index(&self, epoch: u32) -> u32 {
        match self.acked_seq.epoch.cmp(&epoch) {
            Ordering::Less => 0,
            Ordering::Equal => self.acked_seq.index,
            Ordering::Greater => {
                // found the maximum entry of the corresponding epoch.
                todo!()
            }
        }
    }

    pub fn continuously_index(&self, epoch: u32, hole: std::ops::Range<u32>) -> u32 {
        let mut index: u32 = if hole.start == 1 {
            debug_assert!(hole.end > 0);
            hole.end - 1
        } else {
            0
        };
        let iter = self.seek(Sequence::new(epoch, 0));
        for (seq, _) in iter {
            if seq.epoch != epoch || seq.index != index + 1 {
                break;
            }
            index += 1;
            if hole.start == index + 1 {
                index = hole.end - 1;
            }
        }
        index
    }
}

impl<R> PartialStream<R>
where
    R: ReleaseReferringLogFile,
{
    /// Scan a series of entries, returns empty [`VecDeque`] if eof is reached.
    /// A [`None`] is returned if there no more ready entries.
    pub fn scan_entries(
        &self,
        segment_epoch: u32,
        first_index: u32,
        limit: usize,
        require_acked: bool,
    ) -> Result<Option<VecDeque<(u32, Entry)>>> {
        let mut iter = self.seek(Sequence::new(segment_epoch, first_index));
        let first = iter.next();
        if self.is_end_of_epoch_reached(segment_epoch, require_acked, &first) {
            return Ok(Some(VecDeque::default()));
        }

        if first.is_none() {
            return Ok(None);
        }

        let (seq, entry) = first.unwrap();
        if !self.is_next_entry(segment_epoch, require_acked, None, seq) {
            return Ok(None);
        }

        let mut buf = VecDeque::new();
        buf.push_back((seq.index, entry.clone()));
        for (seq, entry) in iter {
            if !self.is_next_entry(segment_epoch, require_acked, buf.back(), seq) {
                break;
            }
            buf.push_back((seq.index, entry.clone()));
            if buf.len() > limit {
                break;
            }
        }
        Ok(Some(buf))
    }

    /// Returns whether the end of epoch has been reached?
    fn is_end_of_epoch_reached(
        &self,
        target_epoch: u32,
        require_acked: bool,
        next: &Option<(&Sequence, &Entry)>,
    ) -> bool {
        let (seq, entry) = match next {
            Some(t) => *t,
            None if !require_acked => {
                // All entries are read, don't wait more entries.
                return true;
            }
            None => {
                return false;
            }
        };

        debug_assert!(target_epoch <= seq.epoch);

        // 1. Already reached next epoch, and a large epoch entry has been acked.
        if target_epoch < seq.epoch && target_epoch < self.acked_seq.epoch {
            return true;
        }

        // 2. An bridge record is acked.
        if let Entry::Bridge { .. } = entry {
            if *seq <= self.acked_seq {
                return true;
            }
        }

        // 3. require un-acked entries, but all entries are consumed.
        if target_epoch < seq.epoch && !require_acked {
            return true;
        }

        false
    }

    fn is_next_entry(
        &self,
        target_epoch: u32,
        require_acked: bool,
        prev: Option<&(u32, Entry)>,
        seq: &Sequence,
    ) -> bool {
        debug_assert!(target_epoch <= seq.epoch);

        // 1. next epoch?
        if target_epoch != seq.epoch {
            return false;
        }

        // 2. is acked?
        if self.acked_seq < *seq && require_acked {
            return false;
        }

        // 3. is continuously?
        if prev
            .map(|(idx, _)| *idx + 1 < seq.index)
            .unwrap_or_default()
        {
            return false;
        }

        true
    }
}

impl<R> PartialStream<R>
where
    R: ReleaseReferringLogFile,
{
    fn reject_staled(&mut self, segment_epoch: u32, writer_epoch: u32) -> IoKindResult<()> {
        if segment_epoch < self.initial_seq.epoch {
            warn!(
                "stream {} seg {} reject staled request, initial epoch is {}",
                self.stream_id, segment_epoch, self.initial_seq.epoch
            );
            return Err(ErrorKind::Other);
        }

        if let Some(sealed_epoch) = self.sealed.get(&segment_epoch) {
            if writer_epoch < *sealed_epoch {
                warn!(
                    "stream {} seg {} reject staled request, writer epoch is {}, sealed epoch is {}",
                    self.stream_id, segment_epoch, writer_epoch, sealed_epoch
                );
                return Err(ErrorKind::Other);
            }
        }
        Ok(())
    }

    fn commit_write_txn(
        &mut self,
        log_number: u64,
        segment_epoch: u32,
        first_index: u32,
        entries: Vec<Entry>,
    ) {
        let mut delta_table = BTreeMap::new();
        for (offset, entry) in entries.into_iter().enumerate() {
            let seq = Sequence::new(segment_epoch, first_index + offset as u32);
            delta_table.insert(seq, entry);
        }

        if self.active_table.is_none() {
            self.active_table = Some((log_number, delta_table));
            return;
        }

        let (active_log_number, active_mem_table) = self.active_table.as_mut().unwrap();
        if *active_log_number == log_number {
            active_mem_table.append(&mut delta_table);
            return;
        }

        // Switch mem table because log file is switched.
        for mem_table in self.stabled_tables.values_mut() {
            mem_table.drain_filter(|seq, _| active_mem_table.contains_key(seq));
        }

        for (log_number, _) in self
            .stabled_tables
            .drain_filter(|_, mem_table| mem_table.is_empty())
        {
            self.log_file_releaser.release(self.stream_id, log_number);
        }

        self.stabled_tables.insert(
            *active_log_number,
            std::mem::replace(active_mem_table, delta_table),
        );
        *active_log_number = log_number;
    }

    fn seek(&self, start_seq: Sequence) -> MemTableIter {
        let mut iters = self
            .stabled_tables
            .iter()
            .map(|(_, m)| m.range(start_seq..).peekable())
            .collect::<Vec<_>>();
        if let Some((_, m)) = &self.active_table {
            iters.push(m.range(start_seq..).peekable());
        }
        MemTableIter::new(start_seq, iters)
    }
}

impl<R> PartialStream<R>
where
    R: ReleaseReferringLogFile,
{
    pub fn refresh_versions(&mut self) {
        if !self.version.try_apply_edits() {
            return;
        }

        // Might update local initial seq, and release useless entries.
        let actual_initial_seq: Sequence = self.version.stream_meta.initial_seq.into();
        if self.initial_seq < actual_initial_seq {
            self.initial_seq = actual_initial_seq;
            self.truncate_entries();
        }
    }

    fn truncate_entries(&mut self) {
        let initial_seq = self.initial_seq;
        for mem_table in self.stabled_tables.values_mut() {
            let mut left = mem_table.split_off(&initial_seq);
            std::mem::swap(&mut left, mem_table);
        }
        let recycled_logs = self
            .stabled_tables
            .drain_filter(|_, mem_table| mem_table.is_empty())
            .map(|v| v.0)
            .collect::<Vec<_>>();
        for log_number in recycled_logs {
            self.log_file_releaser.release(self.stream_id, log_number)
        }
    }
}

struct MemTableIter<'a> {
    next_seq: Sequence,
    /// Iterators in reverse order.
    iters: Vec<Peekable<Range<'a, Sequence, Entry>>>,
}

impl<'a> MemTableIter<'a> {
    fn new(next_seq: Sequence, iters: Vec<Peekable<Range<'a, Sequence, Entry>>>) -> Self {
        MemTableIter { next_seq, iters }
    }
}

impl<'a> std::iter::Iterator for MemTableIter<'a> {
    type Item = (&'a Sequence, &'a Entry);

    fn next(&mut self) -> Option<Self::Item> {
        let mut cached: Option<(&'a Sequence, &'a Entry)> = None;

        'OUTER: for iter in self.iters.iter_mut().rev() {
            while let Some((seq, entry)) = iter.peek() {
                match (*seq).cmp(&self.next_seq) {
                    Ordering::Equal => {
                        cached = iter.next();
                        break 'OUTER;
                    }
                    Ordering::Less => {
                        iter.next();
                        continue;
                    }
                    Ordering::Greater => {
                        if !cached.as_ref().map(|(s, _)| *s <= *seq).unwrap_or_default() {
                            cached = Some((*seq, entry));
                        }
                        break;
                    }
                }
            }
        }

        if let Some((seq, _)) = &cached {
            self.next_seq = Sequence::new(seq.epoch, seq.index + 1);
        }
        cached
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct MockReleaser {}

    impl ReleaseReferringLogFile for MockReleaser {
        fn release(&self, _: u64, _: u64) {}
    }

    fn make_entries(len: usize) -> Vec<Entry> {
        (0..len).into_iter().map(|_| Entry::Hole).collect()
    }

    fn make_sequences(epoch: u32, first: u32, len: usize) -> Vec<Sequence> {
        (0..len)
            .into_iter()
            .map(|i| Sequence::new(epoch, first + i as u32))
            .collect()
    }

    fn submit(
        stream: &mut PartialStream<MockReleaser>,
        log: u64,
        epoch: u32,
        index: u32,
        len: usize,
    ) {
        let txn = TxnContext::Write {
            segment_epoch: epoch,
            first_index: index,
            acked_seq: Sequence::default(),
            prev_acked_seq: Sequence::default(),
            entries: make_entries(len),
        };
        stream.commit(log, txn);
    }

    fn submit_acked_seq(stream: &mut PartialStream<MockReleaser>, log: u64, acked_seq: Sequence) {
        let txn = TxnContext::Write {
            segment_epoch: acked_seq.epoch,
            first_index: acked_seq.index,
            acked_seq,
            prev_acked_seq: Sequence::default(),
            entries: vec![],
        };
        stream.commit(log, txn);
    }

    #[test]
    fn partial_stream_iter() {
        let mut stream = PartialStream::new(StreamVersion::new(1), MockReleaser {});
        // log 1 => [2, 6)  [10, 15)
        submit(&mut stream, 1, 1, 2, 4);
        submit(&mut stream, 1, 1, 10, 4);
        // log 2 => [3, 8)  [10, 18)
        submit(&mut stream, 2, 1, 3, 5);
        submit(&mut stream, 2, 1, 10, 8);
        submit(&mut stream, 2, 2, 10, 8);

        struct TestCase {
            tips: &'static str,
            seek: Sequence,
            expect: Option<Sequence>,
        }

        let cases = vec![
            TestCase {
                tips: "1. out of range",
                seek: Sequence::new(3, 1),
                expect: None,
            },
            TestCase {
                tips: "2. out of range in same epoch",
                seek: Sequence::new(2, 18),
                expect: None,
            },
            TestCase {
                tips: "3. read the smaller one",
                seek: Sequence::new(1, 1),
                expect: Some(Sequence::new(1, 2)),
            },
            TestCase {
                tips: "4. read the recent writes",
                seek: Sequence::new(1, 9),
                expect: Some(Sequence::new(1, 10)),
            },
            TestCase {
                tips: "5. read the equals writes",
                seek: Sequence::new(1, 10),
                expect: Some(Sequence::new(1, 10)),
            },
        ];
        for case in cases {
            let mut iter = stream.seek(case.seek);
            if let Some(expect) = case.expect {
                let next = iter.next();
                assert!(next.is_some(), "in case {}", case.tips);
                let (seq, _) = next.unwrap();
                assert_eq!(expect, *seq, "in case {}", case.tips);
            } else {
                assert!(iter.next().is_none(), "in case {}", case.tips);
            }
        }

        // Iterate all entries.
        let values = stream
            .seek(Sequence::default())
            .map(|(seq, _)| *seq)
            .collect::<Vec<_>>();
        let mut expects = vec![];
        expects.append(&mut make_sequences(1, 2, 6));
        expects.append(&mut make_sequences(1, 10, 8));
        expects.append(&mut make_sequences(2, 10, 8));
        assert_eq!(values, expects,)
    }

    #[test]
    fn partial_stream_scan() {
        const START_INDEX: u32 = 1000;
        const EPOCH: u32 = 100;

        struct TestCase {
            tips: &'static str,
            // [(seq, len) ...]
            input: Vec<(Sequence, usize)>,
            acked_seq: Sequence,
            expects: Option<Vec<u32>>,
            require_acked: bool,
        }

        let range = |start, len| (start..(start + len)).into_iter().collect::<Vec<_>>();

        let cases = vec![
            TestCase {
                tips: "1. return None if not ready",
                input: vec![(Sequence::new(EPOCH, START_INDEX), 10)],
                acked_seq: Sequence::new(0, 0),
                expects: None,
                require_acked: true,
            },
            TestCase {
                tips: "2. return None even if last entry is acked",
                input: vec![(Sequence::new(EPOCH, 1), 50)],
                acked_seq: Sequence::new(EPOCH, START_INDEX),
                expects: None,
                require_acked: true,
            },
            TestCase {
                tips: "3. return empty if eof is reached (next epoch has acked entries)",
                input: vec![
                    (Sequence::new(EPOCH, 1), 50),
                    (Sequence::new(EPOCH + 1, 1), 1),
                ],
                acked_seq: Sequence::new(EPOCH + 1, 1),
                expects: Some(vec![]),
                require_acked: true,
            },
            TestCase {
                tips: "4. only return acked entries if not allow pending entries",
                input: vec![(Sequence::new(EPOCH, START_INDEX), 100)],
                acked_seq: Sequence::new(EPOCH, START_INDEX + 50),
                expects: Some(range(START_INDEX, 51)),
                require_acked: true,
            },
            TestCase {
                tips: "5. allow all entries if pending entries is allowed",
                input: vec![(Sequence::new(EPOCH, START_INDEX), 100)],
                acked_seq: Sequence::new(EPOCH, START_INDEX + 50),
                expects: Some(range(START_INDEX, 100)),
                require_acked: false,
            },
            TestCase {
                tips: "6. return continuously entries",
                input: vec![
                    (Sequence::new(EPOCH, START_INDEX - 50), 100),
                    (Sequence::new(EPOCH, START_INDEX + 500), 100),
                ],
                acked_seq: Sequence::new(EPOCH + 1, 1),
                expects: Some(range(START_INDEX, 50)),
                require_acked: true,
            },
        ];

        for case in cases {
            let mut stream = PartialStream::new(StreamVersion::new(1), MockReleaser {});
            for (seq, len) in case.input {
                submit(&mut stream, 1, seq.epoch, seq.index, len);
            }
            submit_acked_seq(&mut stream, 1, case.acked_seq);
            let entries = stream
                .scan_entries(EPOCH, START_INDEX, usize::MAX, case.require_acked)
                .unwrap_or_else(|_| panic!("in case {}", case.tips));
            if let Some(expects) = case.expects {
                assert!(entries.is_some(), "in case {}", case.tips);
                let read = entries
                    .unwrap()
                    .into_iter()
                    .map(|(index, _)| index)
                    .collect::<Vec<_>>();
                assert_eq!(expects, read, "in case {}", case.tips);
            } else {
                assert!(entries.is_none(), "in case {}", case.tips);
            }
        }
    }
}
