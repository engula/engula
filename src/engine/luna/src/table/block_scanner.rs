// Copyright 2021 The Engula Authors.
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

use std::{cmp::Ordering, sync::Arc};

use bytes::Buf;

use crate::codec::Comparator;

fn read_num_restarts(data: &[u8]) -> u32 {
    debug_assert!(data.len() >= core::mem::size_of::<u32>());

    let offset = data.len() - core::mem::size_of::<u32>();
    (&data[offset..]).get_u32()
}

fn read_restart(restarts: &[u8], index: usize) -> u32 {
    let offset = index * core::mem::size_of::<u32>();
    let end = offset + core::mem::size_of::<u32>();

    debug_assert!(restarts.len() >= end);
    (&restarts[offset..end]).get_u32()
}

fn read_key(data: &[u8], offset: usize) -> &[u8] {
    let end = offset + core::mem::size_of::<u32>();
    let key_len = (&data[offset..end]).get_u32() as usize;
    let key_start = offset + 2 * core::mem::size_of::<u32>();
    let key_end = key_start + key_len;

    &data[key_start..key_end]
}

fn value_range(data: &[u8], offset: usize) -> std::ops::Range<usize> {
    const WIDTH: usize = core::mem::size_of::<u32>();
    let key_len_start = offset;
    let val_len_start = key_len_start + WIDTH;
    let key_start = val_len_start + WIDTH;

    let key_len = (&data[key_len_start..val_len_start]).get_u32() as usize;
    let val_len = (&data[val_len_start..key_start]).get_u32() as usize;
    let val_start = key_start + key_len;
    let val_end = val_start + val_len;
    val_start..val_end
}

fn read_value(data: &[u8], offset: usize) -> &[u8] {
    &data[value_range(data, offset)]
}

fn next_entry_offset(data: &[u8], offset: usize) -> usize {
    value_range(data, offset).end
}

pub struct BlockScanner<C> {
    cmp: C,
    data: Arc<[u8]>,
    num_restarts: usize,
    restart_offset: usize,
    current_offset: usize,
}

impl<C: Comparator> BlockScanner<C> {
    fn next_entry(&mut self) {
        if self.current_offset < self.restart_offset {
            let next_offset = next_entry_offset(&self.data, self.current_offset);
            debug_assert!(next_offset <= self.restart_offset);
            self.current_offset = next_offset;
        }
    }

    fn upper_bound(&self, target: &[u8]) -> usize {
        // [l, r) include the first key large than `target`
        let mut l = 0;
        let mut r = self.num_restarts;
        while l < r {
            let mid = (r - l) / 2 + l;
            let offset = read_restart(&self.data[self.restart_offset..], mid) as usize;
            let key = read_key(&self.data, offset);
            if self.cmp.cmp(key, target) == Ordering::Greater {
                r = mid;
            } else {
                l = mid + 1;
            }
        }
        r
    }
}

#[allow(dead_code)]
impl<C: Comparator> BlockScanner<C> {
    pub fn new(cmp: C, data: Arc<[u8]>) -> Self {
        let num_restarts = read_num_restarts(&data) as usize;
        let tail_len = (num_restarts + 1) * core::mem::size_of::<u32>();
        debug_assert!(num_restarts >= 1);
        debug_assert!(data.len() >= tail_len);

        let restart_offset = data.len() - tail_len;
        BlockScanner {
            cmp,
            data,
            num_restarts,
            restart_offset,
            current_offset: 0,
        }
    }

    pub fn seek_to_first(&mut self) {
        self.current_offset = 0;
    }

    pub fn seek(&mut self, target: &[u8]) {
        let idx = match self.upper_bound(target) {
            0 => 0,
            idx => idx - 1,
        };

        self.current_offset = read_restart(&self.data[self.restart_offset..], idx) as usize;
        while self.valid() {
            if self.cmp.cmp(self.key(), target) != Ordering::Less {
                break;
            }

            self.next_entry();
        }
    }

    pub fn next(&mut self) {
        self.next_entry();
    }

    pub fn valid(&self) -> bool {
        self.current_offset < self.restart_offset
    }

    pub fn key(&self) -> &[u8] {
        assert!(self.valid());
        read_key(&self.data, self.current_offset)
    }

    pub fn value(&self) -> &[u8] {
        assert!(self.valid());
        read_value(&self.data, self.current_offset)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::{codec::BytewiseComparator, table::block_builder::BlockBuilder};

    fn build_block(size: u8) -> Arc<[u8]> {
        let mut builder = BlockBuilder::new(2);
        for id in 1u8..size {
            builder.add(&[id], &[id]);
        }
        builder.finish().into()
    }

    fn match_key(key_opt: &Option<Vec<u8>>, expect: &Option<Vec<u8>>) {
        match &expect {
            Some(v) => assert!(matches!(key_opt, Some(x) if x == v)),
            None => assert!(matches!(key_opt, None)),
        }
    }

    #[tokio::test]
    async fn seek_to_first() {
        let cmp = BytewiseComparator {};
        let data = build_block(128);
        let mut it = BlockScanner::new(cmp, data);
        it.seek_to_first();
        assert!(it.valid());
        assert_eq!(it.key(), vec![1u8]);
    }

    #[tokio::test]
    async fn seek() {
        struct Test {
            target: Vec<u8>,
            expect: Option<Vec<u8>>,
        }
        let tests: Vec<Test> = vec![
            // 1. out of range
            Test {
                target: vec![0u8],
                expect: Some(vec![1u8]),
            },
            Test {
                target: vec![129u8],
                expect: None,
            },
            // 2. binary search boundary
            Test {
                target: vec![1u8],
                expect: Some(vec![1u8]),
            },
            Test {
                target: vec![128u8],
                expect: Some(vec![128u8]),
            },
            // 3. binary search
            Test {
                target: vec![64u8],
                expect: Some(vec![64u8]),
            },
            Test {
                target: vec![65u8],
                expect: Some(vec![65u8]),
            },
            Test {
                target: vec![66u8],
                expect: Some(vec![66u8]),
            },
        ];

        let cmp = BytewiseComparator {};
        let data = build_block(129);
        let mut it = BlockScanner::new(cmp, data);
        for t in tests {
            it.seek(&t.target);
            let key_opt = if it.valid() {
                Some(it.key().to_owned())
            } else {
                None
            };
            match_key(&key_opt, &t.expect);
        }
    }

    #[tokio::test]
    async fn next() {
        struct Test {
            target: Vec<u8>,
            expect: Option<Vec<u8>>,
        }
        let tests: Vec<Test> = vec![
            // 1. out of range
            Test {
                target: vec![0u8],
                expect: Some(vec![2u8]),
            },
            Test {
                target: vec![129u8],
                expect: None,
            },
            // 2. binary search boundary
            Test {
                target: vec![1u8],
                expect: Some(vec![2u8]),
            },
            Test {
                target: vec![128u8],
                expect: None,
            },
            // 3. binary search
            Test {
                target: vec![64u8],
                expect: Some(vec![65u8]),
            },
            Test {
                target: vec![65u8],
                expect: Some(vec![66u8]),
            },
            Test {
                target: vec![66u8],
                expect: Some(vec![67u8]),
            },
            Test {
                target: vec![127u8],
                expect: Some(vec![128u8]),
            },
        ];

        let cmp = BytewiseComparator {};
        let data = build_block(129);
        let mut it = BlockScanner::new(cmp, data);
        for t in tests {
            it.seek(&t.target);
            let key_opt = if it.valid() {
                it.next();
                if it.valid() {
                    Some(it.key().to_owned())
                } else {
                    None
                }
            } else {
                None
            };
            match_key(&key_opt, &t.expect);
        }
    }
}
