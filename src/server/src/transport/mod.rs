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

//! The module of network related operations.

mod discovery;
mod resolver;

use std::sync::Arc;

use engula_client::{
    ClientOptions, ConnManager, EngulaClient, GroupClient, MigrateClient, NodeClient, RootClient,
    Router, RouterGroupState, ShardClient,
};

pub(crate) use self::{discovery::RootDiscovery, resolver::AddressResolver};
use crate::{engine::StateEngine, Result};

#[derive(Clone)]
pub(crate) struct TransportManager {
    address_resolver: Arc<AddressResolver>,
    conn_manager: ConnManager,
    root_client: RootClient,
    router: Router,
}

impl TransportManager {
    pub(crate) async fn new(root_list: Vec<String>, state_engine: StateEngine) -> Self {
        let discovery = Arc::new(RootDiscovery::new(root_list, state_engine));
        let conn_manager = ConnManager::new();
        let root_client = RootClient::new(discovery, conn_manager.clone());
        let router = Router::new(root_client.clone()).await;
        let address_resolver = Arc::new(AddressResolver::new(router.clone()));
        TransportManager {
            address_resolver,
            conn_manager,
            root_client,
            router,
        }
    }

    #[allow(dead_code)]
    #[inline]
    pub(crate) fn conn_manager(&self) -> &ConnManager {
        &self.conn_manager
    }

    #[inline]
    pub(crate) fn root_client(&self) -> &RootClient {
        &self.root_client
    }

    #[inline]
    pub(crate) fn router(&self) -> &Router {
        &self.router
    }

    #[inline]
    pub(crate) fn address_resolver(&self) -> Arc<AddressResolver> {
        self.address_resolver.clone()
    }

    #[inline]
    pub(crate) fn build_client(&self, opts: ClientOptions) -> EngulaClient {
        EngulaClient::build(
            opts,
            self.router.clone(),
            self.root_client.clone(),
            self.conn_manager.clone(),
        )
    }

    #[inline]
    pub(crate) fn lazy_group_client(&self, group_id: u64) -> GroupClient {
        GroupClient::lazy(group_id, self.router.clone(), self.conn_manager.clone())
    }

    #[inline]
    pub(crate) fn get_node_client(&self, addr: String) -> Result<NodeClient> {
        Ok(self.conn_manager.get_node_client(addr)?)
    }

    #[inline]
    pub(crate) fn find_node_client(
        &self,
        node_id: u64,
    ) -> Result<NodeClient, engula_client::Error> {
        let addr = self.router.find_node_addr(node_id)?;
        self.conn_manager.get_node_client(addr)
    }

    #[inline]
    pub(crate) fn build_shard_client(&self, group_id: u64, shard_id: u64) -> ShardClient {
        ShardClient::new(
            group_id,
            shard_id,
            self.router.clone(),
            self.conn_manager.clone(),
        )
    }

    #[inline]
    pub(crate) fn find_group(&self, group_id: u64) -> Result<RouterGroupState> {
        Ok(self.router.find_group(group_id)?)
    }

    #[inline]
    pub(crate) fn build_migrate_client(&self, group_id: u64) -> MigrateClient {
        MigrateClient::new(group_id, self.router.clone(), self.conn_manager.clone())
    }
}
