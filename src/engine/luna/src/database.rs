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

use engula_futures::io::RandomRead;

use crate::{scan::Scan, version::Scanner, Result, WriteBatch};

pub struct Database {}

impl Database {
    pub async fn open() -> Result<Self> {
        todo!();
    }

    pub async fn write(&self, _options: &WriteOptions, _batch: WriteBatch) -> Result<()> {
        todo!();
    }

    pub async fn get(&self, _options: &ReadOptions, _key: &[u8]) -> Result<Option<Vec<u8>>> {
        todo!();
    }

    pub async fn scan<'a, S, R>(&'a self, _options: &ReadOptions) -> Scanner<'a, S, R>
    where
        S: Scan,
        R: RandomRead + 'a,
    {
        todo!();
    }

    pub fn snapshot(&self) -> Snapshot {
        todo!();
    }
}

pub struct Snapshot {}

pub struct ReadOptions {
    _snapshot: Snapshot,
}

pub struct WriteOptions {}
