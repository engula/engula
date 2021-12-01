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

use tokio::io::{AsyncRead, AsyncWrite};

use crate::{async_trait, Result};

/// An interface to manipulate a bucket.
#[async_trait]
pub trait Bucket: Clone + Send + Sync {
    type SequentialReader: AsyncRead + Send + Unpin;
    type SequentialWriter: AsyncWrite + Send + Unpin;

    async fn delete_object(&self, name: &str) -> Result<()>;

    async fn new_sequential_reader(&self, name: &str) -> Result<Self::SequentialReader>;

    async fn new_sequential_writer(&self, name: &str) -> Result<Self::SequentialWriter>;
}
