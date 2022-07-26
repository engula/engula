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

#![feature(cursor_remaining)]
#![feature(drain_filter)]
#![feature(linked_list_cursors)]
#![feature(result_into_ok_or_err)]
#![feature(path_try_exists)]

mod bootstrap;
mod discovery;
mod error;
pub mod node;
mod raftgroup;
mod root;
pub mod runtime;
pub mod serverpb;
mod service;

use std::{path::PathBuf, sync::Arc};

use engula_client::{ConnManager, RootClient, Router};
use node::{resolver::AddressResolver, StateEngine};
use runtime::Executor;
use serde::{Deserialize, Serialize};
use tonic::async_trait;

pub use crate::{
    bootstrap::run,
    error::{Error, Result},
    node::NodeConfig,
    raftgroup::RaftConfig,
    root::{diagnosis, RootConfig},
    service::Server,
};

#[derive(Default, Clone, Debug, Deserialize, Serialize)]
pub struct Config {
    /// The root dir of engula server.
    pub root_dir: PathBuf,

    pub addr: String,

    pub init: bool,

    pub join_list: Vec<String>,

    #[serde(default)]
    pub node: NodeConfig,

    #[serde(default)]
    pub raft: RaftConfig,

    #[serde(default)]
    pub root: RootConfig,
}

pub(crate) struct Provider {
    pub log_path: PathBuf,

    #[allow(unused)]
    pub db_path: PathBuf,

    pub address_resolver: Arc<AddressResolver>,
    pub conn_manager: ConnManager,
    pub executor: Executor,
    pub root_client: RootClient,
    pub router: Router,
    pub raw_db: Arc<rocksdb::DB>,
    pub state_engine: StateEngine,
}

#[cfg(test)]
mod tests {
    #[ctor::ctor]
    fn init() {
        tracing_subscriber::fmt::init();
    }
}
