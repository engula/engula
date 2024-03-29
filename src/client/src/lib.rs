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

#![feature(map_try_insert)]

mod app_client;
mod conn_manager;
mod discovery;
pub mod error;
mod group_client;
mod metrics;
mod migrate_client;
mod node_client;
mod retry;
mod root_client;
mod router;
mod shard_client;

pub use app_client::{Client as EngulaClient, ClientOptions, Collection, Database, Partition};
pub use conn_manager::ConnManager;
pub use discovery::{ServiceDiscovery, StaticServiceDiscovery};
pub use error::{AppError, AppResult, Error, Result};
pub use group_client::{GroupClient, RetryableShardChunkStreaming};
pub use migrate_client::MigrateClient;
pub use node_client::{Client as NodeClient, RequestBatchBuilder, RpcTimeout};
pub use retry::RetryState;
pub use root_client::{AdminRequestBuilder, AdminResponseExtractor, Client as RootClient};
pub use router::{Router, RouterGroupState};
pub use shard_client::ShardClient;
use tonic::async_trait;
