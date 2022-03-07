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

use crate::Result;

pub struct BucketIter {}

impl BucketIter {
    pub fn id(&self) -> &[u8] {
        todo!();
    }

    pub fn value(&self) -> &[u8] {
        todo!();
    }

    pub fn valid(&self) -> bool {
        todo!();
    }

    pub async fn seek_to_first(&mut self) -> Result<()> {
        todo!();
    }

    pub async fn seek(&mut self, _target: &[u8]) -> Result<()> {
        todo!();
    }

    pub async fn next(&mut self) -> Result<()> {
        todo!();
    }
}
