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

use std::collections::{HashMap, VecDeque};

use super::Policy;
use crate::Entry;

#[derive(Debug, Clone)]
enum ReaderState {
    Polling,
    Ready { index: u32, entry: Entry },
    Done,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum GroupPolicy {
    /// A simple strategy that accept entries as long as one copy holds the
    /// dataset.
    ///
    /// This strategy is mainly used for testing.
    Simple,

    /// A strategy that accept a majority set of entries are ready.
    ///
    /// This strategy is mainly used for testing.
    #[allow(unused)]
    Majority,
}

impl From<Policy> for GroupPolicy {
    fn from(policy: Policy) -> Self {
        match policy {
            Policy::Simple => GroupPolicy::Simple,
        }
    }
}

struct ReadingProgress {
    reader_state: ReaderState,
    /// Whether the end of reading is reached.
    terminated: bool,
    entries: VecDeque<(u32, Entry)>,
}

impl ReadingProgress {
    fn new() -> Self {
        ReadingProgress {
            reader_state: ReaderState::Polling,
            terminated: false,
            entries: VecDeque::new(),
        }
    }

    fn is_ready(&self) -> bool {
        self.terminated || !self.entries.is_empty()
    }

    /// Append entries into progress, if entries is empty, the end of reading is
    /// reached.
    fn append(&mut self, entries: Vec<(u32, Entry)>) {
        if entries.is_empty() {
            self.terminated = true;
        } else {
            self.entries.append(&mut entries.into());
        }
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Copy)]
enum GroupState {
    Pending,
    Active,
    Done,
}

#[allow(unused)]
pub(crate) struct GroupReader {
    reading_progress: HashMap<String, ReadingProgress>,
    num_ready: usize,
    num_done: usize,
    epoch: u32,
    next_index: u32,
    policy: GroupPolicy,
}

#[allow(dead_code)]
impl GroupReader {
    pub(super) fn new(
        policy: GroupPolicy,
        epoch: u32,
        next_index: u32,
        copy_set: Vec<String>,
    ) -> Self {
        GroupReader {
            reading_progress: copy_set
                .into_iter()
                .map(|t| (t, ReadingProgress::new()))
                .collect(),
            num_ready: 0,
            num_done: 0,
            epoch,
            next_index,
            policy,
        }
    }

    fn majority(&self) -> usize {
        (self.reading_progress.len() / 2) + 1
    }

    fn state(&self) -> GroupState {
        let num_copies = self.reading_progress.len();
        let majority = self.majority();
        match self.policy {
            GroupPolicy::Simple if self.num_done == num_copies => GroupState::Done,
            GroupPolicy::Simple if self.num_ready >= 1 => GroupState::Active,
            GroupPolicy::Majority if self.num_ready >= majority => GroupState::Active,
            GroupPolicy::Majority if self.num_done >= majority => GroupState::Done,
            GroupPolicy::Majority if self.num_ready + self.num_done >= majority => {
                GroupState::Active
            }
            _ if self.num_done == num_copies => GroupState::Done,
            _ => GroupState::Pending,
        }
    }

    /// Read next entry of group state, panic if this isn't active
    fn next_entry(&mut self) -> Option<Entry> {
        let mut fresh_entry: Option<Entry> = None;
        for state in self
            .reading_progress
            .iter_mut()
            .map(|(_, p)| &mut p.reader_state)
        {
            // Found matched index
            if let ReaderState::Ready { index, entry } = state {
                if *index == self.next_index {
                    self.num_ready -= 1;

                    if !fresh_entry
                        .as_ref()
                        .map(|e| e.epoch() >= entry.epoch())
                        .unwrap_or_default()
                    {
                        fresh_entry = Some(std::mem::replace(entry, Entry::Hole));
                    }
                    *state = ReaderState::Polling;
                }
            }
        }

        // skip to next
        self.next_index += 1;

        fresh_entry
    }

    fn consume_learned_entries(&mut self) {
        for progress in self.reading_progress.values_mut() {
            if let ReaderState::Polling = progress.reader_state {
                if !progress.is_ready() {
                    continue;
                }

                match progress.entries.pop_front() {
                    Some((index, entry)) if index >= self.next_index => {
                        self.num_ready += 1;
                        progress.reader_state = ReaderState::Ready { index, entry };
                    }
                    Some(_) => {
                        // Ignore staled entries.
                    }
                    None => {
                        self.num_done += 1;
                        progress.reader_state = ReaderState::Done;
                    }
                }
            }
        }
    }

    pub(crate) fn append(&mut self, target: &str, entries: Vec<(u32, Entry)>) {
        if let Some(progress) = self.reading_progress.get_mut(target) {
            progress.append(entries);
        }
    }

    pub(crate) fn take_next_entry(&mut self) -> Option<(u32, Entry)> {
        self.consume_learned_entries();
        let next_index = self.next_index;
        match self.state() {
            GroupState::Active => self
                .next_entry()
                .map(|e| (next_index, e))
                .or(Some((next_index, Entry::Hole))),
            GroupState::Done => Some((next_index, Entry::Bridge { epoch: self.epoch })),
            GroupState::Pending => None,
        }
    }

    /// Return the next index this target haven't receiving.
    pub(crate) fn target_next_index(&self, target: &str) -> u32 {
        self.reading_progress
            .get(target)
            .and_then(|p| p.entries.back().map(|e| e.0 + 1))
            .unwrap_or(self.next_index)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Group reader must reject staled (index less than next_index)
    /// transforming request.
    #[test]
    fn group_reader_ignore_staled_request() {
        let mut reader = GroupReader::new(GroupPolicy::Simple, 1, 123, vec!["c".to_string()]);

        reader.append("c", vec![(122, Entry::Hole)]);
        let state = &reader.reading_progress.get_mut("c").unwrap().reader_state;
        assert!(matches!(state, ReaderState::Polling));
        assert_eq!(reader.num_ready, 0);
        assert_eq!(reader.num_done, 0);
    }

    fn ee(index: u32, epoch: u32) -> Entry {
        let event: Vec<u8> = index.to_le_bytes().as_slice().into();
        Entry::Event {
            epoch,
            event: event.into(),
        }
    }

    #[test]
    fn group_reader_next_entry_basic() {
        struct TestCase {
            desc: &'static str,
            states: Vec<ReaderState>,
            expects: Vec<Option<Entry>>,
        }

        let cases = vec![
            TestCase {
                desc: "1. return largest entry",
                states: vec![
                    ReaderState::Ready {
                        index: 1,
                        entry: ee(2, 2),
                    },
                    ReaderState::Ready {
                        index: 1,
                        entry: ee(2, 1),
                    },
                    ReaderState::Ready {
                        index: 1,
                        entry: ee(2, 3),
                    },
                ],
                expects: vec![Some(ee(2, 3))],
            },
            TestCase {
                desc: "2. allow pending state",
                states: vec![
                    ReaderState::Polling,
                    ReaderState::Ready {
                        index: 1,
                        entry: ee(2, 2),
                    },
                    ReaderState::Ready {
                        index: 1,
                        entry: ee(2, 1),
                    },
                ],
                expects: vec![Some(ee(2, 2))],
            },
            TestCase {
                desc: "3. returns hole if no such index entry exists",
                states: vec![
                    ReaderState::Ready {
                        index: 2,
                        entry: ee(2, 2),
                    },
                    ReaderState::Ready {
                        index: 4,
                        entry: ee(4, 2),
                    },
                    ReaderState::Ready {
                        index: 6,
                        entry: ee(6, 8),
                    },
                ],
                expects: vec![
                    None,
                    Some(ee(2, 2)),
                    None,
                    Some(ee(4, 2)),
                    None,
                    Some(ee(6, 8)),
                ],
            },
        ];

        for case in cases {
            let mut reader = GroupReader::new(
                GroupPolicy::Simple,
                1,
                1,
                vec!["a".to_string(), "b".to_string(), "c".to_string()],
            );
            reader.num_ready = 3;
            for expect in case.expects {
                let mut it = case.states.iter();
                for progress in reader.reading_progress.values_mut() {
                    progress.reader_state = it.next().unwrap().clone();
                }
                let entry = reader.next_entry();
                match expect {
                    Some(e) => {
                        assert!(entry.is_some(), "case: {}", case.desc);
                        assert_eq!(entry.unwrap(), e, "case: {}", case.desc);
                    }
                    None => {
                        assert!(entry.is_none(), "case: {}", case.desc);
                    }
                }
            }
        }
    }

    #[test]
    fn group_reader_state() {
        #[derive(Debug)]
        struct TestCase {
            num_copies: usize,
            num_ready: usize,
            num_done: usize,
            group_policy: GroupPolicy,
            expect_state: GroupState,
        }
        let cases = vec![
            // 1. simple policy pending
            TestCase {
                num_copies: 1,
                num_ready: 0,
                num_done: 0,
                group_policy: GroupPolicy::Simple,
                expect_state: GroupState::Pending,
            },
            // 2. simple policy active
            TestCase {
                num_copies: 1,
                num_ready: 1,
                num_done: 0,
                group_policy: GroupPolicy::Simple,
                expect_state: GroupState::Active,
            },
            // 3. simple policy done
            TestCase {
                num_copies: 1,
                num_ready: 0,
                num_done: 1,
                group_policy: GroupPolicy::Simple,
                expect_state: GroupState::Done,
            },
            // 4. simple policy active but some copies already done.
            TestCase {
                num_copies: 2,
                num_ready: 1,
                num_done: 1,
                group_policy: GroupPolicy::Simple,
                expect_state: GroupState::Active,
            },
            // 5. majority policy pending
            TestCase {
                num_copies: 3,
                num_ready: 1,
                num_done: 0,
                group_policy: GroupPolicy::Majority,
                expect_state: GroupState::Pending,
            },
            // 6. majority policy active
            TestCase {
                num_copies: 3,
                num_ready: 2,
                num_done: 0,
                group_policy: GroupPolicy::Majority,
                expect_state: GroupState::Active,
            },
            // 7. majority policy active but partial done
            TestCase {
                num_copies: 3,
                num_ready: 1,
                num_done: 1,
                group_policy: GroupPolicy::Majority,
                expect_state: GroupState::Active,
            },
            // 8. majority policy done
            TestCase {
                num_copies: 3,
                num_ready: 0,
                num_done: 2,
                group_policy: GroupPolicy::Majority,
                expect_state: GroupState::Done,
            },
            // 9. majority policy active although majority done, this is expected for recovering.
            TestCase {
                num_copies: 3,
                num_ready: 1,
                num_done: 2,
                group_policy: GroupPolicy::Majority,
                expect_state: GroupState::Done,
            },
        ];
        let copies = vec![
            "a".to_string(),
            "b".to_string(),
            "c".to_string(),
            "d".to_string(),
        ];
        for case in cases {
            let mut reader =
                GroupReader::new(case.group_policy, 1, 1, copies[..case.num_copies].to_vec());
            reader.num_ready = case.num_ready;
            reader.num_done = case.num_done;
            assert_eq!(reader.state(), case.expect_state, "{:?}", case);
        }
    }
}
