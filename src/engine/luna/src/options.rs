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

use crate::codec::Timestamp;

pub struct Options {
    pub memtable_size: usize,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            memtable_size: 8 * 1024 * 1024,
        }
    }
}

pub struct Snapshot {
    pub(crate) ts: Timestamp,
}

pub struct ReadOptions {
    pub snapshot: Snapshot,
}

impl Default for ReadOptions {
    fn default() -> Self {
        let snapshot = Snapshot { ts: Timestamp::MAX };
        Self { snapshot }
    }
}
#[derive(Default)]
pub struct WriteOptions {}
