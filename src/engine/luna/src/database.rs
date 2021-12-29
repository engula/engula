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

use crate::{Iter, Result, WriteBatch};

pub struct Database {}

impl Database {
    pub async fn open() -> Result<Self> {
        Ok(Self {})
    }

    pub async fn get(&self, _: &[u8]) -> Result<Option<Vec<u8>>> {
        unimplemented!();
    }

    pub async fn iter(&self) -> Result<Iter> {
        unimplemented!();
    }

    pub async fn write(&self, _: WriteBatch) -> Result<()> {
        unimplemented!();
    }
}
