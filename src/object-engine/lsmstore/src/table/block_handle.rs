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

use std::mem::size_of;

use bytes::BufMut;

pub struct BlockHandle {
    pub offset: u64,
    pub length: u64,
}

impl BlockHandle {
    pub fn encode_to<B: BufMut>(&self, buf: &mut B) {
        buf.put_u64(self.offset);
        buf.put_u64(self.length);
    }

    pub fn encode_to_vec(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(size_of::<u64>() * 2);
        self.encode_to(&mut buf);
        buf
    }
}
