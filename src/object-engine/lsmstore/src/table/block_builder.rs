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

// Block format:
//
// Block = { Entry } Footer
// Entry = {
//     key size   : fixed32
//     value size : fixed32
//     key        : key size bytes
//     value      : value size bytes
// }
// Footer = {
//     restarts     : fixed32 * num_restarts
//     num_restarts : fixed32
// }

#[allow(dead_code)]
pub struct BlockBuilder {
    buf: Vec<u8>,
    restarts: Vec<u32>,
    num_entries: usize,
    restart_interval: usize,
}

impl Default for BlockBuilder {
    fn default() -> Self {
        Self {
            buf: Vec::new(),
            restarts: Vec::new(),
            num_entries: 0,
            restart_interval: 8,
        }
    }
}

#[allow(dead_code)]
impl BlockBuilder {
    pub fn restart_interval(mut self, interval: usize) -> Self {
        self.restart_interval = interval;
        self
    }

    pub fn reset(&mut self) {
        self.buf.clear();
        self.restarts.clear();
        self.num_entries = 0;
    }

    pub fn add(&mut self, key: &[u8], value: &[u8]) {
        if self.num_entries % self.restart_interval == 0 {
            self.restarts.push(self.buf.len() as u32);
        }

        // TODO: add prefix compression
        self.buf.put_u32(key.len() as u32);
        self.buf.put_u32(value.len() as u32);
        self.buf.put_slice(key);
        self.buf.put_slice(value);
        self.num_entries += 1;
    }

    pub fn finish(&mut self) -> &[u8] {
        for restart in &self.restarts {
            self.buf.put_u32(*restart);
        }
        self.buf.put_u32(self.restarts.len() as u32);
        &self.buf
    }

    pub fn num_entries(&self) -> usize {
        self.num_entries
    }

    pub fn encoded_size(&self) -> usize {
        self.buf.len() + self.restarts.len() * size_of::<u32>() + size_of::<u32>()
    }
}
