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

use std::{
    sync::{Arc, Mutex},
    task::Poll,
    time::Duration,
};

pub use self::schema::Schema;
use self::store::RootStore;
use crate::{
    node::{Node, Replica, ReplicaRouteTable},
    runtime::{Executor, TaskPriority},
    Error, Result,
};

#[derive(Clone)]
pub struct Root {
    shared: Arc<RootShared>,
}

struct RootShared {
    executor: Executor,
    cluster_id: Vec<u8>,
    local_addr: String,
    core: Mutex<Option<RootCore>>,
}

struct RootCore {
    schema: Arc<Schema>,
}

impl Root {
    pub fn new(executor: Executor, cluster_id: Vec<u8>, local_addr: String) -> Self {
        Self {
            shared: Arc::new(RootShared {
                executor,
                cluster_id,
                local_addr,
                core: Mutex::new(None),
            }),
        }
    }

    pub fn is_root(&self) -> bool {
        self.shared.core.lock().unwrap().is_some()
    }

    pub async fn bootstrap(&mut self, node: &Node) -> Result<()> {
        let replica_table = node.replica_table().clone();
        let root = self.clone();
        self.shared
            .executor
            .spawn(None, TaskPriority::Middle, async move {
                root.run(replica_table).await;
            });
        Ok(())
    }

    pub fn schema(&self) -> Option<Arc<Schema>> {
        let core = self.shared.core.lock().unwrap();
        core.as_ref().map(|c| c.schema.clone())
    }

    async fn run(&self, replica_table: ReplicaRouteTable) -> ! {
        let mut bootstrapped = false;
        loop {
            let root_replica = self.fetch_root_replica(&replica_table).await;

            // Wait the current root replica becomes a leader.
            if root_replica.on_leader().await.is_ok() {
                match self
                    .step_leader(&self.shared.local_addr, root_replica, &mut bootstrapped)
                    .await
                {
                    Ok(()) | Err(Error::NotLeader(_, _)) => {
                        // Step follower
                        continue;
                    }
                    Err(err) => {
                        todo!("handle error: {}", err)
                    }
                }
            }
        }
    }

    async fn fetch_root_replica(&self, replica_table: &ReplicaRouteTable) -> Arc<Replica> {
        use futures::future::poll_fn;
        poll_fn(
            |ctx| match replica_table.current_root_replica(Some(ctx.waker().clone())) {
                Some(root_replica) => Poll::Ready(root_replica),
                None => Poll::Pending,
            },
        )
        .await
    }

    async fn step_leader(
        &self,
        local_addr: &str,
        root_replica: Arc<Replica>,
        bootstrapped: &mut bool,
    ) -> Result<()> {
        let store = Arc::new(RootStore::new(root_replica));
        let mut schema = Schema::new(store.clone());

        // Only when the program is initialized is it checked for bootstrap, after which the
        // leadership change does not need to check for whether bootstrap or not.
        if !*bootstrapped {
            schema
                .try_bootstrap(local_addr, self.shared.cluster_id.clone())
                .await?;
            *bootstrapped = true;
        }

        {
            let mut core = self.shared.core.lock().unwrap();
            *core = Some(RootCore {
                schema: Arc::new(schema),
            });
        }

        // TODO(zojw): refresh owner, heartbeat node, rebalance
        for _ in 0..1000 {
            tokio::time::sleep(Duration::from_secs(1)).await;
        }

        // After that, RootCore needs to be set to None before returning.
        {
            let mut core = self.shared.core.lock().unwrap();
            *core = None;
        }

        Ok(())
    }
}

#[cfg(test)]
mod test_root {
    #[tokio::test]
    async fn name() {}
}
