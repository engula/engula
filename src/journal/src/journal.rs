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

use engula_futures::stream::batch::ResultStream;

use crate::{async_trait, Error, Result, StreamReader, StreamWriter};

/// A stream storage abstraction.
#[async_trait]
pub trait Journal {
    type StreamLister: ResultStream<Elem = String, Error = Error>;
    type StreamReader: StreamReader;
    type StreamWriter: StreamWriter;

    /// Lists streams.
    async fn list_streams(&self) -> Result<Self::StreamLister>;

    /// Creates a stream.
    ///
    /// # Errors
    ///
    /// Returns `Error::AlreadyExists` if the stream already exists.
    async fn create_stream(&self, name: &str) -> Result<()>;

    /// Deletes a stream.
    ///
    /// Using a deleted stream is an undefined behavior.
    ///
    /// # Errors
    ///
    /// Returns `Error::NotFound` if the stream doesn't exist.
    async fn delete_stream(&self, name: &str) -> Result<()>;

    /// Returns a stream reader.
    async fn new_stream_reader(&self, name: &str) -> Result<Self::StreamReader>;

    /// Returns a stream writer.
    async fn new_stream_writer(&self, name: &str) -> Result<Self::StreamWriter>;
}
