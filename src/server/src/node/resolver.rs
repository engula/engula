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

use crate::{Result, Error};

pub struct AddressResolver {}

#[crate::async_trait]
impl crate::node::replica::raft::AddressResolver for AddressResolver {
    async fn resolve(&self, _node_id: u64) -> Result<NodeDesc> {
        Err(Error::InvalidArgument("not such node exists".into()))
    }
}
