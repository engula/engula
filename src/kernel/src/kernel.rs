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

use engula_futures::io::{RandomRead, SequentialWrite};
use engula_journal::{StreamReader, StreamWriter};

use crate::{async_trait, KernelUpdate, Result, Sequence};

/// A stateful environment for storage engines.
#[async_trait]
pub trait Kernel {
    type UpdateReader: UpdateReader;
    type UpdateWriter: UpdateWriter;
    type StreamReader: StreamReader;
    type StreamWriter: StreamWriter;
    type RandomReader: RandomRead;
    type SequentialWriter: SequentialWrite;

    /// Returns a reader to read updates.
    async fn new_update_reader(&self) -> Result<Self::UpdateReader>;

    /// Returns a writer to update the kernel.
    async fn new_update_writer(&self) -> Result<Self::UpdateWriter>;

    async fn new_stream_reader(&self, stream_name: &str) -> Result<Self::StreamReader>;

    async fn new_stream_writer(&self, stream_name: &str) -> Result<Self::StreamWriter>;

    async fn new_random_reader(
        &self,
        bucket_name: &str,
        object_name: &str,
    ) -> Result<Self::RandomReader>;

    async fn new_sequential_writer(
        &self,
        bucket_name: &str,
        object_name: &str,
    ) -> Result<Self::SequentialWriter>;
}

pub type UpdateEvent = (Sequence, KernelUpdate);

#[async_trait]
pub trait UpdateReader {
    /// Returns the next update event if it is available.
    async fn try_next(&mut self) -> Result<Option<UpdateEvent>>;

    /// Returns the next update event or waits until it is available.
    async fn wait_next(&mut self) -> Result<UpdateEvent>;
}

#[async_trait]
pub trait UpdateWriter {
    /// Appends an update, returns the sequence of the update just append.
    async fn append(&mut self, update: KernelUpdate) -> Result<Sequence>;

    /// Releases updates up to a sequence (exclusive).
    ///
    /// Some operations of an update are not executed until the update is
    /// released. For example, if an update that deletes an object is appended,
    /// the object will be marked as deleted but it will still be valid for
    /// reads until the update is released. This allows users to finish
    /// ongoing requests even if some objects are marked as deleted. However,
    /// this is not a guarantee but a best-effort optimization. The
    /// implementation can still delete the objects in some cases, for example,
    /// if the user fails to keep alive with the kernel.
    async fn release(&mut self, sequence: Sequence) -> Result<()>;
}
