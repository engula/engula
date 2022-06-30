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

use std::collections::HashSet;

use engula_api::server::v1::{
    watch_response::{update_event, UpdateEvent},
    *,
};
use engula_client::NodeClient;
use tracing::warn;

use super::{
    allocator::{GroupAction, ReplicaAction},
    Root, Schema,
};
use crate::Result;

impl Root {
    pub async fn send_heartbeat(&self, schema: Schema) -> Result<()> {
        let cur_node_id = self.current_node_id();
        let nodes = schema.list_node().await?;

        let mut piggybacks = Vec::new();

        // TODO: no need piggyback root info everytime.
        if true {
            let mut roots = schema.get_root_replicas().await?;
            roots.move_first(cur_node_id);
            piggybacks.push(PiggybackRequest {
                info: Some(piggyback_request::Info::SyncRoot(SyncRootRequest {
                    roots: roots.into(),
                })),
            });
            piggybacks.push(PiggybackRequest {
                info: Some(piggyback_request::Info::CollectGroupDetail(
                    CollectGroupDetailRequest { groups: vec![] },
                )),
            });
            piggybacks.push(PiggybackRequest {
                info: Some(piggyback_request::Info::CollectStats(CollectStatsRequest {
                    field_mask: None,
                })),
            });
        }

        // TODO: collect stats and group detail.

        for n in nodes {
            match Self::try_send_heartbeat(&n.addr, &piggybacks).await {
                Ok(res) => {
                    for resp in res.piggybacks {
                        match resp.info.unwrap() {
                            piggyback_response::Info::SyncRoot(_) => {}
                            piggyback_response::Info::CollectStats(resp) => {
                                self.handle_collect_stats(&schema, resp, n.id).await?
                            }
                            piggyback_response::Info::CollectGroupDetail(resp) => {
                                self.handle_group_detail(&schema, resp).await?
                            }
                        }
                    }
                }
                Err(err) => {
                    warn!("heartbeat to node {} address {}: {}", n.id, n.addr, err);
                }
            }
        }
        Ok(())
    }

    async fn try_send_heartbeat(
        addr: &str,
        piggybacks: &[PiggybackRequest],
    ) -> Result<HeartbeatResponse> {
        let client = NodeClient::connect(addr.to_owned()).await?;
        let resp = client
            .root_heartbeat(HeartbeatRequest {
                piggybacks: piggybacks.to_owned(),
                timestamp: 0, // TODO: use hlc
            })
            .await?;
        Ok(resp)
    }

    async fn handle_collect_stats(
        &self,
        schema: &Schema,
        resp: CollectStatsResponse,
        node_id: u64,
    ) -> Result<()> {
        if let Some(ns) = resp.node_stats {
            if let Some(mut node) = schema.get_node(node_id).await? {
                let mut cap = node.capacity.take().unwrap();
                cap.replica_count = ns.group_count as u64;
                cap.leader_count = ns.leader_count as u64;
                node.capacity = Some(cap);
                schema.update_node(node).await?;
            }
        }
        Ok(())
    }

    async fn handle_group_detail(
        &self,
        schema: &Schema,
        resp: CollectGroupDetailResponse,
    ) -> Result<()> {
        let mut update_events = Vec::new();

        for desc in &resp.group_descs {
            if let Some(ex) = schema.get_group(desc.id).await? {
                if desc.epoch <= ex.epoch {
                    continue;
                }
            }
            schema
                .update_group_replica(Some(desc.to_owned()), None)
                .await?;
            update_events.push(UpdateEvent {
                event: Some(update_event::Event::Group(desc.to_owned())),
            })
        }

        let mut changed_group_states = HashSet::new();
        for state in &resp.replica_states {
            if let Some(ex) = schema
                .get_replica_state(state.group_id, state.replica_id)
                .await?
            {
                if state.term <= ex.term {
                    continue;
                }
            }
            schema
                .update_group_replica(None, Some(state.to_owned()))
                .await?;
            changed_group_states.insert(state.group_id);
        }

        let mut states = schema.list_group_state().await?; // TODO: fix poor performance.
        states.retain(|s| changed_group_states.contains(&s.group_id));
        for state in states {
            update_events.push(UpdateEvent {
                event: Some(update_event::Event::GroupState(state)),
            })
        }

        if !update_events.is_empty() {
            self.watcher_hub().notify_updates(update_events).await;
        }

        Ok(())
    }
}

impl Root {
    #[allow(dead_code)]
    pub async fn reconcile_group(&self) -> Result<()> {
        let group_action = self.alloc.compute_group_action().await?;
        match group_action {
            GroupAction::Noop => {}
            GroupAction::Add(cnt) => self.create_groups(cnt).await?,
            GroupAction::Remove(_) => {
                // TODO
            }
        }

        let replica_actions = self.alloc.compute_replica_action().await?;
        for replica_action in replica_actions {
            match replica_action {
                ReplicaAction::Noop => {}
                ReplicaAction::Migrate(_action) => { // TODO:
                }
            }
        }

        let shard_actions = self.alloc.compute_shard_action().await?;
        for shard_action in shard_actions {
            match shard_action {
                super::allocator::ShardAction::Noop => {}
                super::allocator::ShardAction::Migrate(_action) => {}
            }
        }

        Ok(())
    }
}
