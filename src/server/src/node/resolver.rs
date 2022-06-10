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
use engula_client::RootClient;
use tracing::warn;

use crate::{Error, Result};

pub struct AddressResolver {
    root_list: Vec<String>,
    nodes: Mutex<HashMap<u64, NodeDesc>>,
}

impl AddressResolver {
    pub fn new(root_list: Vec<String>) -> Self {
        AddressResolver {
            root_list,
            nodes: Mutex::default(),
        }
    }

    pub fn insert(&self, desc: &NodeDesc) {
        let mut nodes = self.nodes.lock().unwrap();
        nodes.insert(desc.id, desc.to_owned());
    }

    pub fn find(&self, node_id: u64) -> Option<NodeDesc> {
        let nodes = self.nodes.lock().unwrap();
        nodes.get(&node_id).cloned()
    }

    async fn issue_resolve_request(target_addr: &str, node_id: u64) -> Result<Option<NodeDesc>> {
        let client = RootClient::connect(target_addr.to_string()).await?;
        let node_desc = client.resolve(node_id).await?;
        Ok(node_desc)
    }
}

#[crate::async_trait]
impl crate::node::replica::raft::AddressResolver for AddressResolver {
    async fn resolve(&self, node_id: u64) -> Result<NodeDesc> {
        if let Some(desc) = self.find(node_id) {
            return Ok(desc);
        }

        for addr in &self.root_list {
            match Self::issue_resolve_request(addr, node_id).await {
                Ok(resp) => match resp {
                    Some(desc) => return Ok(desc),
                    None => continue,
                },
                Err(e) => {
                    warn!(err = ?e, root = ?addr, "issue resolve request to root server");
                }
            }
        }

        Err(Error::InvalidArgument("no such node exists".into()))
    }
}
