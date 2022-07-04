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

use engula_api::server::v1::NodeDesc;
use engula_client::Router;

use crate::{Error, Result};

pub struct AddressResolver {
    router: Router,
}

impl AddressResolver {
    pub fn new(router: Router) -> Self {
        AddressResolver { router }
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

        Err(Error::InvalidArgument("no such node exists".into()))
    }
}
