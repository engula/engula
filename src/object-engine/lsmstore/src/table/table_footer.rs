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

use bytes::{Buf, BufMut};

use super::{block_handle, BlockHandle};
use crate::{Error, Result};

pub struct TableFooter {
    pub index_handle: BlockHandle,
}

pub const ENCODED_SIZE: usize = block_handle::ENCODED_SIZE;

impl TableFooter {
    pub fn encode_to<B: BufMut>(&self, buf: &mut B) {
        self.index_handle.encode_to(buf);
    }

    pub fn encode_to_vec(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(ENCODED_SIZE);
        self.encode_to(&mut buf);
        buf
    }

    pub fn decode_from<B: Buf>(buf: &mut B) -> Result<Self> {
        if buf.remaining() >= ENCODED_SIZE {
            let index_handle = BlockHandle::decode_from(buf);
            Ok(Self { index_handle })
        } else {
            Err(Error::corrupted("table footer is too small"))
        }
    }
}
