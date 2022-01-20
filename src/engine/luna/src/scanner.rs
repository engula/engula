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
    codec::{InternalKey, ParsedInternalKey, Timestamp, ValueKind},
    version::InternalScanner,
    Result,
};

pub struct Scanner {
    scanner: InternalScanner,
    snapshot: Timestamp,
    last_key: Option<Vec<u8>>,
}

impl Scanner {
    pub fn new(scanner: InternalScanner, snapshot: Timestamp) -> Self {
        Self {
            scanner,
            snapshot,
            last_key: None,
        }
    }

    async fn skip_to_visible_forward(&mut self) -> Result<()> {
        while self.scanner.valid() {
            let pk = ParsedInternalKey::decode_from(self.scanner.key());
            if pk.timestamp <= self.snapshot
                && (self.last_key.is_none() || pk.user_key != self.last_key.as_ref().unwrap())
            {
                self.last_key = Some(pk.user_key.to_owned());
                if pk.value_kind == ValueKind::Some {
                    break;
                }
            }
            self.scanner.next().await?;
        }
        Ok(())
    }

    pub async fn seek_to_first(&mut self) -> Result<()> {
        self.scanner.seek_to_first().await?;
        self.last_key = None;
        self.skip_to_visible_forward().await
    }

    pub async fn seek(&mut self, target: &[u8]) -> Result<()> {
        let lookup_key = InternalKey::for_lookup(target, self.snapshot);
        self.scanner.seek(lookup_key.as_bytes()).await?;
        self.last_key = None;
        self.skip_to_visible_forward().await?;
        Ok(())
    }

    pub async fn next(&mut self) -> Result<()> {
        self.scanner.next().await?;
        self.skip_to_visible_forward().await
    }

    pub fn valid(&self) -> bool {
        self.scanner.valid()
    }

    pub fn key(&self) -> &[u8] {
        self.last_key.as_ref().unwrap()
    }

    pub fn value(&self) -> &[u8] {
        self.scanner.value()
    }
}
