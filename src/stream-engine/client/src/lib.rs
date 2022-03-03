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

#![feature(result_into_ok_or_err)]

#[macro_use]
extern crate derivative;

mod core;
mod engine;
mod group;
mod master;
mod policy;
mod reader;
mod store;
mod stream;
mod tenant;

pub use stream_engine_common::{
    error::{Error, Result},
    Entry, Sequence,
};

pub use self::{
    engine::Engine,
    stream::{EpochState, Role, Stream},
    tenant::Tenant,
};

type TonicResult<T> = std::result::Result<T, tonic::Status>;
