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

use std::{mem::size_of, ops::Range, sync::Arc};

use bytes::Buf;

use super::Key;

pub struct BlockIter {
    block: Arc<[u8]>,
    num_restarts: usize,
    restarts_offset: usize,
    key_range: Range<usize>,
    value_range: Range<usize>,
}

impl BlockIter {
    pub fn new(block: Arc<[u8]>) -> Self {
        let offset = block.len() - size_of::<u32>();
        let num_restarts = (&block[offset..]).get_u32() as usize;
        let restarts_offset = offset - num_restarts * size_of::<u32>();
        Self {
            block,
            num_restarts,
            restarts_offset,
            key_range: Range::default(),
            value_range: Range::default(),
        }
    }

    pub fn key(&self) -> Key<'_> {
        debug_assert!(self.valid());
        self.block[self.key_range.clone()].into()
    }

    pub fn value(&self) -> &[u8] {
        debug_assert!(self.valid());
        &self.block[self.value_range.clone()]
    }

    pub fn valid(&self) -> bool {
        !self.key_range.is_empty()
    }

    pub fn seek_to_first(&mut self) {
        self.decode_next(0);
    }

    pub fn seek(&mut self, target: Key<'_>) {
        let mut l = 0;
        let mut r = self.num_restarts - 1;
        while l < r {
            let m = (l + r + 1) / 2;
            let k = self.restart_key(m);
            if k <= target {
                l = m;
            } else {
                r = m - 1;
            }
        }

        self.decode_next(self.restart_offset(l));
        while self.valid() {
            if self.key() >= target {
                break;
            }
            self.next();
        }
    }

    pub fn next(&mut self) {
        self.decode_next(self.next_offset())
    }

    fn next_offset(&self) -> usize {
        self.value_range.end
    }

    fn decode_next(&mut self, offset: usize) {
        if offset < self.restarts_offset {
            (self.key_range, self.value_range) = decode_entry(&self.block, offset);
        } else {
            (self.key_range, self.value_range) = (Range::default(), Range::default());
        }
    }

    fn restart_key(&self, index: usize) -> Key<'_> {
        let offset = self.restart_offset(index);
        let (key_range, _) = decode_entry(&self.block, offset);
        self.block[key_range].into()
    }

    fn restart_offset(&self, index: usize) -> usize {
        let offset = self.restarts_offset + index * size_of::<u32>();
        (&self.block[offset..]).get_u32() as usize
    }
}

fn decode_entry(buf: &[u8], mut offset: usize) -> (Range<usize>, Range<usize>) {
    let mut buf = &buf[offset..];
    let key_size = buf.get_u32() as usize;
    let value_size = buf.get_u32() as usize;
    offset += size_of::<u32>() * 2;
    let key_range = offset..(offset + key_size);
    offset += key_size;
    let value_range = offset..(offset + value_size);
    (key_range, value_range)
}
