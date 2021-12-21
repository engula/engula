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

use bytes::{Buf, BufMut};

// BlockHandle format:
//   offset : fixed64
//   length : fixed64

pub struct BlockHandle {
    pub offset: u64,
    pub length: u64,
}

impl BlockHandle {
    pub fn new(offset: u64, length: u64) -> Self {
        Self { offset, length }
    }

    pub fn encode_to(&self, buf: &mut impl BufMut) {
        buf.put_u64(self.offset);
        buf.put_u64(self.length);
    }

    pub fn encode_to_vec(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(16);
        self.encode_to(&mut buf);
        buf
    }

    #[allow(dead_code)]
    pub fn decode_from(buf: &mut impl Buf) -> Self {
        let offset = buf.get_u64();
        let length = buf.get_u64();
        Self { offset, length }
    }
}
