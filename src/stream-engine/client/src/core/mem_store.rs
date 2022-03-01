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

use std::collections::VecDeque;

use crate::{Entry, Sequence};

/// Store entries for a stream.
pub(super) struct MemStore {
    epoch: u32,

    /// It should always greater than zero, see `journal::worker::Progress` for
    /// details.
    first_index: u32,
    base_bytes: usize,
    total_bytes: usize,
    entries: VecDeque<(Entry, usize)>,
}

impl MemStore {
    pub fn new(epoch: u32) -> Self {
        MemStore {
            epoch,
            first_index: 1,
            base_bytes: 0,
            total_bytes: 0,
            entries: VecDeque::new(),
        }
    }

    pub fn recovery(epoch: u32, first_index: u32) -> Self {
        MemStore {
            first_index,
            ..MemStore::new(epoch)
        }
    }

    /// Returns the index of next entry.
    pub fn next_index(&self) -> u32 {
        self.first_index + self.entries.len() as u32
    }

    /// Save entry and assign index.
    pub fn append(&mut self, entry: Entry) -> Sequence {
        let next_index = self.next_index();
        self.total_bytes += entry.size();
        self.entries.push_back((entry, self.total_bytes));
        Sequence::new(self.epoch, next_index)
    }

    /// Range values.
    pub fn range(&self, mut r: std::ops::Range<u32>, quota: usize) -> Option<(Vec<Entry>, usize)> {
        let target_bytes = self.base_bytes + quota + 1;
        let next_index = self.next_index();
        r.end = r.end.min(next_index);
        if r.start < r.end && self.first_index <= r.start {
            let start = (r.start - self.first_index) as usize;
            let end = (r.end - self.first_index) as usize;

            let mut entries = vec![];
            let mut bytes = 0;
            for (entry, accumulated_bytes) in self.entries.range(start..end) {
                if *accumulated_bytes > target_bytes {
                    break;
                }
                bytes = *accumulated_bytes;
                entries.push(entry.clone());
            }
            Some((entries, bytes))
        } else {
            None
        }
    }

    /// Drain useless entries
    #[allow(dead_code)]
    pub fn release(&mut self, until: u32) {
        let next_index = self.next_index();
        if self.first_index < until && until < next_index {
            let offset = until - self.first_index;
            self.base_bytes += self
                .entries
                .drain(..offset as usize)
                .map(|(_, bytes)| bytes)
                .sum::<usize>();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mem_storage_append() {
        let mut mem_storage = MemStore::new(0);
        for idx in 0..128 {
            let seq = mem_storage.append(Entry::Event {
                epoch: 0,
                event: Box::new([0u8]),
            });
            assert_eq!(<Sequence as Into<u64>>::into(seq), idx + 1);
        }
    }

    #[test]
    fn mem_storage_range() {
        #[derive(Debug)]
        struct Test {
            entries: Vec<Entry>,
            range: std::ops::Range<u32>,
            expect: Option<Vec<Entry>>,
        }

        let ent = |v| Entry::Event {
            epoch: 1,
            event: vec![v].into(),
        };

        let tests = vec![
            // 1. empty request
            Test {
                entries: vec![],
                range: 1..1,
                expect: None,
            },
            // 2. empty request and out of range
            Test {
                entries: vec![],
                range: 2..2,
                expect: None,
            },
            // 3. single entry
            Test {
                entries: vec![ent(1)],
                range: 1..2,
                expect: Some(vec![ent(1)]),
            },
            // 4. out of range
            Test {
                entries: vec![ent(1)],
                range: 2..3,
                expect: None,
            },
            // 5. partially covered
            Test {
                entries: vec![ent(1), ent(2)],
                range: 2..3,
                expect: Some(vec![ent(2)]),
            },
            // 6. totally covered
            Test {
                entries: vec![ent(1), ent(2)],
                range: 1..3,
                expect: Some(vec![ent(1), ent(2)]),
            },
            // 7. out of range but partial covered
            Test {
                entries: vec![ent(1), ent(2)],
                range: 1..4,
                expect: Some(vec![ent(1), ent(2)]),
            },
            Test {
                entries: vec![ent(1), ent(2)],
                range: 2..4,
                expect: Some(vec![ent(2)]),
            },
        ];

        for test in tests {
            let mut mem_store = MemStore::new(1);
            for entry in test.entries {
                mem_store.append(entry);
            }
            let got = mem_store.range(test.range, 4096000);
            match test.expect {
                Some(entries) => {
                    assert!(got.is_some());
                    assert!(entries
                        .into_iter()
                        .zip(got.unwrap().0.into_iter())
                        .all(|(l, r)| l == r));
                }
                None => {
                    assert!(got.is_none());
                }
            }
        }
    }

    #[test]
    fn mem_storage_range_with_quota() {
        #[derive(Debug)]
        struct Test {
            quota: usize,
            expect: Option<Vec<Entry>>,
        }

        let ent = |v: usize| Entry::Event {
            epoch: 1,
            event: v.to_le_bytes().into(),
        };

        let cases = vec![
            Test {
                quota: 10,
                expect: Some(vec![ent(1)]),
            },
            Test {
                quota: 20,
                expect: Some(vec![ent(1), ent(2)]),
            },
            Test {
                quota: 30,
                expect: Some(vec![ent(1), ent(2), ent(3)]),
            },
        ];

        let mut mem_store = MemStore::new(1);
        for i in 1..1024 {
            mem_store.append(ent(i));
        }

        for case in cases {
            let got = mem_store.range(1..1024, case.quota);
            match case.expect {
                Some(entries) => {
                    assert!(got.is_some());
                    assert!(entries
                        .into_iter()
                        .zip(got.unwrap().0.into_iter())
                        .all(|(l, r)| l == r));
                }
                None => {
                    assert!(got.is_none());
                }
            }
        }
    }
}
