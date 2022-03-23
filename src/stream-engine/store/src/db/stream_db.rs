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
    collections::{HashMap, VecDeque},
    ffi::OsString,
    path::Path,
    sync::{Arc, Mutex},
    task::Context,
};

use stream_engine_proto::Record;

use super::{
    partial_stream::{PartialStream, TxnContext},
    pipeline::{PipelinedWriter, WriterOwner},
    reader::SegmentReader,
};
use crate::{
    fs::layout,
    log::{LogEngine, LogFileManager},
    DbOption, Entry, Error, Result, Sequence,
};

fn convert_to_delta(record: &Record) -> (u64, TxnContext) {
    if let Some(writer_epoch) = &record.writer_epoch {
        (
            record.stream_id,
            TxnContext::Sealed {
                segment_epoch: record.epoch,
                writer_epoch: *writer_epoch,
                prev_epoch: None,
            },
        )
    } else {
        (
            record.stream_id,
            TxnContext::Write {
                segment_epoch: record.epoch,
                first_index: record.first_index.unwrap(),
                acked_seq: record.acked_seq.unwrap().into(),
                prev_acked_seq: Sequence::new(0, 0),
                entries: record.entries.iter().cloned().map(Into::into).collect(),
            },
        )
    }
}

struct DbLayout {
    max_file_number: u64,
    log_numbers: Vec<u64>,
    obsoleted_files: Vec<OsString>,
}

fn analyze_db_layout<P: AsRef<Path>>(base_dir: P, manifest_file_number: u64) -> Result<DbLayout> {
    let mut max_file_number: u64 = 0;
    let mut log_numbers = vec![];
    let mut obsoleted_files = vec![];
    for dir_entry in std::fs::read_dir(&base_dir)? {
        let dir_entry = dir_entry?;
        let path = dir_entry.path();
        if !path.is_file() {
            continue;
        }
        match layout::parse_file_name(&path)? {
            layout::FileType::Current => continue,
            layout::FileType::Unknown => {}
            layout::FileType::Temp => obsoleted_files.push(path.file_name().unwrap().to_owned()),
            layout::FileType::Manifest(number) => {
                max_file_number = max_file_number.max(number);
                if number != manifest_file_number {
                    obsoleted_files.push(path.file_name().unwrap().to_owned());
                }
            }
            layout::FileType::Log(number) => {
                max_file_number = max_file_number.max(number);
                log_numbers.push(number);
            }
        }
    }
    Ok(DbLayout {
        max_file_number,
        log_numbers,
        obsoleted_files,
    })
}

fn recover_log_engine<P: AsRef<Path>>(
    base_dir: P,
    opt: Arc<DbOption>,
    db_layout: &mut DbLayout,
) -> Result<(LogEngine, HashMap<u64, PartialStream<LogFileManager>>)> {
    let log_file_mgr = LogFileManager::new(&base_dir, db_layout.max_file_number + 1, opt);
    log_file_mgr.recycle_all(vec![]);
    let mut streams: HashMap<u64, PartialStream<_>> = HashMap::new();
    let mut applier = |log_number, record| {
        let (stream_id, delta) = convert_to_delta(&record);
        let stream = streams
            .entry(stream_id)
            .or_insert_with(|| PartialStream::new(stream_id, log_file_mgr.clone()));
        stream.commit(log_number, delta);
        Ok(())
    };
    let log_engine = LogEngine::recover(
        base_dir,
        db_layout.log_numbers.clone(),
        log_file_mgr.clone(),
        &mut applier,
    )?;
    Ok((log_engine, streams))
}

fn remove_obsoleted_files(db_layout: DbLayout) {
    for name in db_layout.obsoleted_files {
        if let Err(err) = std::fs::remove_file(&name) {
            tracing::warn!("remove obsoleted file {:?}: {}", name, err);
        } else {
            tracing::info!("obsoleted file {:?} is removed", name);
        }
    }
}

struct StreamDbCore {
    streams: HashMap<u64, StreamMixin>,
}

#[derive(Clone)]
pub struct StreamDb {
    log_engine: LogEngine,
    core: Arc<Mutex<StreamDbCore>>,
}

impl StreamDb {
    pub fn open<P: AsRef<Path>>(base_dir: P, opt: DbOption) -> Result<StreamDb> {
        std::fs::create_dir_all(&base_dir)?;
        let opt = Arc::new(opt);

        // TODO(walter) add file lock.
        if !layout::current(&base_dir).try_exists()? {
            if !opt.create_if_missing {
                return Err(Error::NotFound(format!(
                    "stream database {}",
                    base_dir.as_ref().display()
                )));
            }

            // Create new DB instance then recover it.
            Self::create(&base_dir)?;
        }

        Self::recover(base_dir, opt)
    }

    fn recover<P: AsRef<Path>>(base_dir: P, opt: Arc<DbOption>) -> Result<StreamDb> {
        // TODO(walter) recover version set.
        let mut db_layout = analyze_db_layout(&base_dir, 0)?;
        let (log_engine, streams) = recover_log_engine(&base_dir, opt, &mut db_layout)?;
        remove_obsoleted_files(db_layout);
        let streams = streams
            .into_iter()
            .map(|(stream_id, partial_stream)| {
                (
                    stream_id,
                    StreamMixin::new(stream_id, partial_stream, log_engine.clone()),
                )
            })
            .collect();

        Ok(StreamDb {
            log_engine,
            core: Arc::new(Mutex::new(StreamDbCore { streams })),
        })
    }

    #[inline(always)]
    fn create<P: AsRef<Path>>(_base_dir: P) -> Result<()> {
        // TODO(walter) VersionSet::create(base_dir)
        Ok(())
    }

    #[inline(always)]
    pub async fn write(
        &self,
        stream_id: u64,
        seg_epoch: u32,
        writer_epoch: u32,
        acked_seq: Sequence,
        first_index: u32,
        entries: Vec<Entry>,
    ) -> Result<(u32, u32)> {
        self.must_get_stream(stream_id)
            .write(seg_epoch, writer_epoch, acked_seq, first_index, entries)
            .await
    }

    #[inline(always)]
    pub fn read(
        &self,
        stream_id: u64,
        seg_epoch: u32,
        start_index: u32,
        limit: usize,
        require_acked: bool,
    ) -> Result<SegmentReader> {
        let stream = {
            let core = self.core.lock().unwrap();
            match core.streams.get(&stream_id) {
                Some(s) => s.clone(),
                None => return Err(Error::NotFound(format!("stream {}", stream_id))),
            }
        };
        Ok(SegmentReader::new(
            seg_epoch,
            start_index,
            limit,
            require_acked,
            stream,
        ))
    }

    #[inline(always)]
    pub async fn seal(&self, stream_id: u64, seg_epoch: u32, writer_epoch: u32) -> Result<u32> {
        self.must_get_stream(stream_id)
            .seal(seg_epoch, writer_epoch)
            .await
    }

    #[inline(always)]
    fn must_get_stream(&self, stream_id: u64) -> StreamMixin {
        use std::ops::DerefMut;

        let mut core = self.core.lock().unwrap();
        let core = core.deref_mut();
        core.streams
            .entry(stream_id)
            .or_insert_with(|| StreamMixin::new_empty(stream_id, self.log_engine.clone()))
            .clone()
    }
}

#[derive(Clone)]
pub(crate) struct StreamMixin {
    core: Arc<Mutex<StreamCore>>,
}

pub(crate) struct StreamCore {
    storage: PartialStream<LogFileManager>,
    writer: PipelinedWriter,
}

impl StreamMixin {
    fn new(stream_id: u64, storage: PartialStream<LogFileManager>, log_engine: LogEngine) -> Self {
        let writer = PipelinedWriter::new(stream_id, log_engine);
        StreamMixin {
            core: Arc::new(Mutex::new(StreamCore { storage, writer })),
        }
    }

    fn new_empty(stream_id: u64, log_engine: LogEngine) -> Self {
        let storage = PartialStream::new(stream_id, log_engine.log_file_manager());
        Self::new(stream_id, storage, log_engine)
    }

    async fn write(
        &self,
        seg_epoch: u32,
        writer_epoch: u32,
        acked_seq: Sequence,
        first_index: u32,
        entries: Vec<Entry>,
    ) -> Result<(u32, u32)> {
        let (index, acked_index, waiter) = {
            let num_entries = entries.len() as u32;
            let mut core = self.core.lock().unwrap();
            let txn = core
                .storage
                .write(seg_epoch, writer_epoch, acked_seq, first_index, entries);
            let continuously_index = core
                .storage
                .continuously_index(seg_epoch, first_index..(first_index + num_entries));
            let acked_index = core.storage.acked_index(seg_epoch);
            (
                continuously_index,
                acked_index,
                core.writer.submit(self.core.clone(), txn),
            )
        };

        waiter.await?;
        Ok((index, acked_index))
    }

    #[inline(always)]
    async fn seal(&self, seg_epoch: u32, writer_epoch: u32) -> Result<u32> {
        let (acked_index, waiter) = {
            let mut core = self.core.lock().unwrap();
            let txn = core.storage.seal(seg_epoch, writer_epoch);
            let acked_index = core.storage.acked_index(seg_epoch);
            (acked_index, core.writer.submit(self.core.clone(), txn))
        };

        waiter.await?;

        Ok(acked_index)
    }
}

impl StreamMixin {
    /// Poll entries from start_index, if the entries aren't ready for
    /// reading, a [`None`] is returned, and a [`std::task::Waker`] is taken.
    pub fn poll_entries(
        &self,
        cx: &mut Context<'_>,
        required_epoch: u32,
        start_index: u32,
        limit: usize,
        require_acked: bool,
    ) -> Result<Option<VecDeque<(u32, Entry)>>> {
        let mut core = self.core.lock().unwrap();
        if let Some(entries_container) =
            core.storage
                .scan_entries(required_epoch, start_index, limit, require_acked)?
        {
            Ok(Some(entries_container))
        } else {
            core.writer.register_reading_waiter(cx.waker().clone());
            Ok(None)
        }
    }
}

impl WriterOwner for StreamCore {
    fn borrow_pipelined_writer_mut(
        &mut self,
    ) -> (&mut PartialStream<LogFileManager>, &mut PipelinedWriter) {
        (&mut self.storage, &mut self.writer)
    }
}
