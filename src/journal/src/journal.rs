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

use super::{async_trait, stream::Stream};

/// An interface to manipulate a journal.
#[async_trait]
pub trait Journal<S: Stream> {
    /// Returns a stream.
    async fn stream(&self, name: &str) -> Result<S, S::Error>;

    /// Creates a stream.
    async fn create_stream(&self, name: &str) -> Result<S, S::Error>;

    /// Deletes a stream.
    async fn delete_stream(&self, name: &str) -> Result<(), S::Error>;
}
