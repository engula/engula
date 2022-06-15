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
use std::sync::Arc;

use crate::{
    node::{resolver::AddressResolver, Node},
    root::{Root, WatchHub},
};

pub mod node;
pub mod raft;
pub mod root;

#[derive(Clone)]
pub struct Server {
    pub node: Arc<Node>,
    pub root: Root,
    pub address_resolver: Arc<AddressResolver>,
    pub watcher_hub: Arc<WatchHub>,
}
