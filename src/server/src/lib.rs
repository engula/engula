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
#![feature(result_into_ok_or_err)]
#![feature(path_try_exists)]

mod bootstrap;
mod error;
pub mod node;
mod raftgroup;
mod root;
pub mod runtime;
pub mod serverpb;
mod service;

pub use tonic::async_trait;

pub use crate::{
    bootstrap::run,
    error::{Error, Result},
    service::Server,
};
