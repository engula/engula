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

use crate::{
    codec::{self, Timestamp, Value},
    Result,
};

#[derive(Default)]
pub struct WriteBatch {
    ts: Timestamp,
    writes: Vec<(Vec<u8>, Value)>,
}

impl WriteBatch {
    pub fn put(&mut self, key: &[u8], value: &[u8]) -> &mut Self {
        self.writes.push((key.to_owned(), Some(value.to_owned())));
        self
    }

    pub fn delete(&mut self, key: &[u8]) -> &mut Self {
        self.writes.push((key.to_owned(), None));
        self
    }

    pub(crate) fn timestamp(&self) -> Timestamp {
        self.ts
    }

    pub(crate) fn set_timestamp(&mut self, ts: Timestamp) {
        self.ts = ts;
    }

    pub(crate) fn encode_to_vec(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        codec::put_timestamp(&mut buf, self.ts);
        for w in &self.writes {
            codec::put_length_prefixed_slice(&mut buf, &w.0);
            codec::put_value(&mut buf, &w.1);
        }
        buf
    }

    pub(crate) fn decode_from(_buf: &[u8]) -> Result<WriteBatch> {
        todo!();
    }
}
