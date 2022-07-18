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

use std::{collections::HashMap, sync::Mutex};

use engula_api::server::v1::NodeDesc;
use engula_client::Router;

use crate::{Error, Result};

pub struct AddressResolver {
    router: Router,
    initial_nodes: Mutex<HashMap<u64, String>>,
}

impl AddressResolver {
    pub fn new(router: Router) -> Self {
        AddressResolver {
            router,
            initial_nodes: Mutex::default(),
        }
    }

    pub fn set_initial_nodes(&self, initial_nodes: Vec<NodeDesc>) {
        let mut guard = self.initial_nodes.lock().unwrap();
        *guard = initial_nodes
            .into_iter()
            .map(|n| (n.id, n.addr))
            .collect::<HashMap<_, _>>();
    }
}

#[crate::async_trait]
impl crate::raftgroup::AddressResolver for AddressResolver {
    async fn resolve(&self, node_id: u64) -> Result<NodeDesc> {
        if let Ok(addr) = self.router.find_node_addr(node_id) {
            return Ok(NodeDesc {
                id: node_id,
                addr,
                ..Default::default()
            });
        }

        let initial_nodes = self.initial_nodes.lock().unwrap();
        if let Some(addr) = initial_nodes.get(&node_id) {
            return Ok(NodeDesc {
                id: node_id,
                addr: addr.clone(),
                ..Default::default()
            });
        }

        Err(Error::InvalidArgument("no such node exists".into()))
    }
}
