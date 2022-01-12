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

use async_trait::async_trait;

use crate::{Result, Sequence};

#[derive(Debug)]
pub struct Writer {}

#[async_trait]
impl crate::StreamWriter for Writer {
    /// Appends an event, returns the sequence of the event just append.
    async fn append(&mut self, _event: Vec<u8>) -> Result<Sequence> {
        todo!();
    }

    /// Truncates events up to a sequence (exclusive).
    async fn truncate(&mut self, _sequence: Sequence) -> Result<()> {
        todo!();
    }
}
