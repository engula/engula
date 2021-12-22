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

use std::mem::size_of;

use bytes::BufMut;

// Block format:
//
// Block = { Entry } Footer
//
// Entry format:
//   key size   : fixed32
//   value size : fixed32
//   key        : key size
//   value      : value size
//
// Footer format:
//   restarts     : fixed32 * num_restarts
//   num_restarts : fixed32

pub struct BlockBuilder {
    buf: Vec<u8>,
    restarts: Vec<u32>,
    num_entries: u32,
    restart_interval: u32,
}

impl BlockBuilder {
    pub fn new(restart_interval: u32) -> Self {
        Self {
            buf: Vec::new(),
            restarts: Vec::new(),
            num_entries: 0,
            restart_interval,
        }
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

    pub fn num_entries(&self) -> u32 {
        self.num_entries
    }

    pub fn estimated_size(&self) -> u32 {
        (self.buf.len() + self.restarts.len() * size_of::<u32>() + size_of::<u32>()) as u32
    }
}
