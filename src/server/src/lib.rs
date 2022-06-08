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

#![feature(drain_filter)]

mod bootstrap;
mod error;
pub mod node;
mod root;
pub mod runtime;
mod service;

pub use raft::eraftpb;
pub use tonic::async_trait;

pub use crate::{
    bootstrap::run,
    error::{Error, Result},
    service::Server,
};

pub mod serverpb {
    pub mod v1 {
        #![allow(clippy::all)]
        tonic::include_proto!("serverpb.v1");
    }
}
