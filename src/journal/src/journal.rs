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

use async_trait::async_trait;

use crate::{error::Result, journal_stream::JournalStream};

/// An interface to manipulate journal streams.
#[async_trait]
pub trait Journal {
    /// Returns a journal stream.
    async fn stream(&self, name: &str) -> Result<Box<dyn JournalStream>>;

    /// Returns the names of all journal streams.
    async fn list_streams(&self) -> Result<Vec<String>>;

    /// Creates a journal stream.
    async fn create_stream(&self, name: &str) -> Result<Box<dyn JournalStream>>;

    /// Deletes a journal stream.
    async fn delete_stream(&self, name: &str) -> Result<()>;
}
