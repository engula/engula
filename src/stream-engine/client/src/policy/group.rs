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

use super::Policy;
use crate::Entry;

#[derive(Debug, Clone)]
pub(crate) enum ReaderState {
    Polling,
    Ready { index: u32, entry: Entry },
    Done,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum GroupPolicy {
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

#[allow(unused)]
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Copy)]
pub(crate) enum GroupState {
    Pending,
    Active,
    Done,
}

#[allow(unused)]
#[derive(Debug)]
pub(crate) struct GroupReader {
    num_ready: usize,
    num_done: usize,
    num_copies: usize,
    next_index: u32,
    policy: GroupPolicy,
}

#[allow(dead_code)]
impl GroupReader {
    pub(super) fn new(policy: GroupPolicy, next_index: u32, num_copies: usize) -> Self {
        GroupReader {
            num_ready: 0,
            num_done: 0,
            num_copies,
            next_index,
            policy,
        }
    }

    fn majority(&self) -> usize {
        (self.num_copies / 2) + 1
    }

    pub(super) fn state(&self) -> GroupState {
        let majority = self.majority();
        match self.policy {
            GroupPolicy::Simple if self.num_ready >= 1 => GroupState::Active,
            GroupPolicy::Majority if self.num_ready >= majority => GroupState::Active,
            GroupPolicy::Majority if self.num_done >= majority => GroupState::Done,
            GroupPolicy::Majority if self.num_ready + self.num_done >= majority => {
                GroupState::Active
            }
            _ if self.num_done == self.num_copies => GroupState::Done,
            _ => GroupState::Pending,
        }
    }

    #[inline(always)]
    pub(super) fn next_index(&self) -> u32 {
        self.next_index
    }

    pub(super) fn transform(
        &mut self,
        reader_state: &mut ReaderState,
        input: Option<(u32, Entry)>,
    ) {
        match input {
            Some((index, entry)) if index >= self.next_index => {
                self.num_ready += 1;
                *reader_state = ReaderState::Ready { index, entry };
            }
            Some(_) => {
                // Ignore staled entries.
            }
            None => {
                self.num_done += 1;
                *reader_state = ReaderState::Done;
            }
        }
    }

    /// Read next entry of group state, panic if this isn't active
    pub(super) fn next_entry<'a, I>(&mut self, i: I) -> Option<Entry>
    where
        I: IntoIterator<Item = &'a mut ReaderState>,
    {
        // Found matched index
        let mut fresh_entry: Option<Entry> = None;
        for state in i.into_iter() {
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
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Group reader must reject staled (index less than next_index)
    /// transforming request.
    #[test]
    fn group_reader_ignore_staled_request() {
        let mut reader = GroupReader::new(GroupPolicy::Simple, 123, 3);
        let mut state = ReaderState::Polling;

        reader.transform(&mut state, Some((122, Entry::Hole)));
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

        for mut case in cases {
            let mut reader = GroupReader::new(GroupPolicy::Simple, 1, 3);
            reader.num_ready = 3;
            for expect in case.expects {
                let entry = reader.next_entry(case.states.iter_mut());
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
        for case in cases {
            let mut reader = GroupReader::new(case.group_policy, 1, case.num_copies);
            reader.num_ready = case.num_ready;
            reader.num_done = case.num_done;
            assert_eq!(reader.state(), case.expect_state, "{:?}", case);
        }
    }
}
