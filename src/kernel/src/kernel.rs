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

use engula_journal::{StreamReader, StreamWriter};
use engula_runtime::io::{RandomRead, SequentialWrite};

use crate::{async_trait, KernelUpdate, KernelUpdateReader, Result};

/// An interface to interact with a kernel.
#[async_trait]
pub trait Kernel<T>: Clone + Send + Sync + 'static {
    type KernelUpdateReader: KernelUpdateReader;
    type StreamReader: StreamReader<T>;
    type StreamWriter: StreamWriter<T>;
    type RandomObjectReader: RandomRead;
    type SequentialObjectWriter: SequentialWrite;

    /// Applies a kernel update.
    async fn apply_update(&self, update: KernelUpdate) -> Result<()>;

    /// Returns a reader to receive kernel updates.
    async fn new_update_reader(&self) -> Result<Self::KernelUpdateReader>;

    async fn new_stream_reader(&self, stream_name: &str) -> Result<Self::StreamReader>;

    async fn new_stream_writer(&self, stream_name: &str) -> Result<Self::StreamWriter>;

    async fn new_random_object_reader(
        &self,
        bucket_name: &str,
        object_name: &str,
    ) -> Result<Self::RandomObjectReader>;

    async fn new_sequential_object_writer(
        &self,
        bucket_name: &str,
        object_name: &str,
    ) -> Result<Self::SequentialObjectWriter>;
}
