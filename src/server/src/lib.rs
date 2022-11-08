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
#![feature(fs_try_exists)]
#![feature(type_name_of_val)]
#![feature(const_type_name)]

mod bootstrap;
mod config;
mod constants;
mod engine;
mod error;
mod root;
mod schedule;
mod service;
mod transport;

pub mod node;
pub mod raftgroup;
pub mod runtime;
pub mod serverpb;

pub(crate) use tonic::async_trait;

pub use crate::{
    bootstrap::run,
    config::*,
    error::{Error, Result},
    node::NodeConfig,
    raftgroup::RaftConfig,
    root::{diagnosis, RootConfig},
    runtime::ExecutorConfig,
    service::Server,
};

#[cfg(test)]
mod tests {
    #[ctor::ctor]
    fn init() {
        tracing_subscriber::fmt::init();
    }
}
