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

mod job;
mod schema;
mod store;

use std::sync::Arc;

use self::{schema::Schema, store::RootStore};
use crate::{
    node::Node,
    runtime::{Executor, TaskPriority},
    Error, Result,
};

pub struct Root {
    is_root: bool,
    executor: Executor,
    schema: Option<Arc<Schema>>,
    cluster_id: Option<Vec<u8>>,
}

impl Root {
    pub fn new(executor: Executor) -> Self {
        Self {
            is_root: false,
            executor,
            schema: None,
            cluster_id: None,
        }
    }

    pub fn is_root(&self) -> bool {
        self.is_root
    }

    pub async fn bootstrap(&mut self, node: &Node, addr: &str, cluster_id: Vec<u8>) -> Result<()> {
        let root_replica = node.replica_table().current_root_replica().unwrap();
        let store = Arc::new(RootStore::new(root_replica));
        let mut schema = Schema::new(store.clone());

        schema.try_bootstrap(addr, cluster_id.to_owned()).await?;

        self.cluster_id = Some(cluster_id);

        self.refresh_root_owner(node);
        self.executor
            .spawn(None, TaskPriority::Middle, async move {}); // TODO(zojw): refresh owner, heartbeat node, rebalance
        Ok(())
    }

    pub fn schema(&self) -> Result<Arc<Schema>> {
        self.schema.to_owned().ok_or(Error::NotRootLeader)
    }

    fn refresh_root_owner(&mut self, node: &Node) {
        let store = node
            .replica_table()
            .current_root_replica()
            .map(RootStore::new);
        if let Some(store) = store {
            let schema = Schema::new(Arc::new(store));
            self.schema = Some(Arc::new(schema));
            self.is_root = true;
        } else {
            self.schema = None;
            self.is_root = false;
        }
    }
}

#[cfg(test)]
mod test_root {
    #[tokio::test]
    async fn name() {}
}
