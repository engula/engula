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

pub mod apis;
mod args;
mod collection;
mod cooperator;
mod database;
mod server;
mod universe;
mod write_batch;

use engula_common::{Error, Result};

use self::{
    args::Args,
    collection::Collection,
    database::Database,
    universe::Universe,
    write_batch::{Write, WriteBatch},
};
pub use self::{cooperator::Cooperator, server::Server};
