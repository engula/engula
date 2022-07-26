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

use std::{collections::HashSet, ops::Add, sync::Arc, time::Duration, vec};

use engula_api::server::v1::{
    watch_response::{update_event, UpdateEvent},
    *,
};
use futures::future::join_all;
use tokio::time::Instant;
use tracing::{info, trace, warn};

use super::{HeartbeatTask, Root, Schema};
use crate::{root::schema::ReplicaNodes, Result};

impl Root {
    pub async fn send_heartbeat(&self, schema: Arc<Schema>, tasks: &[HeartbeatTask]) -> Result<()> {
        let cur_node_id = self.current_node_id();
        let nodes = schema.list_node().await?;
        let nodes = nodes
            .iter()
            .filter(|n| tasks.iter().any(|t| t.node_id == n.id))
            .collect::<Vec<_>>();

        info!("sending heartbeat to {:?}", &nodes);

        let mut piggybacks = Vec::new();

        // TODO: no need piggyback root info everytime.
        if true {
            let mut root = schema.get_root_desc().await?;
            root.root_nodes = {
                let mut nodes = ReplicaNodes(root.root_nodes);
                nodes.move_first(cur_node_id);
                nodes.0
            };
            trace!(
                root = ?root.root_nodes.iter().map(|n| n.id).collect::<Vec<_>>(),
                "sync root info with heartbeat"
            );
            piggybacks.push(PiggybackRequest {
                info: Some(piggyback_request::Info::SyncRoot(SyncRootRequest {
                    root: Some(root),
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

        let mut futs = Vec::new();
        for n in &nodes {
            trace!(node = n.id, target = ?n.addr, "attempt send heartbeat");
            let fut = self.try_send_heartbeat(
                n.addr.to_owned(),
                &piggybacks,
                Duration::from_secs(self.cfg.heartbeat_timeout_sec),
            );
            futs.push(fut);
        }

        let resps = join_all(futs).await;
        let last_heartbeat = Instant::now();
        for (i, resp) in resps.iter().enumerate() {
            let n = nodes.get(i).unwrap();
            match resp {
                Ok(res) => {
                    self.liveness.renew(n.id);
                    for resp in &res.piggybacks {
                        match resp.info.as_ref().unwrap() {
                            piggyback_response::Info::SyncRoot(_)
                            | piggyback_response::Info::CollectMigrationState(_) => {}
                            piggyback_response::Info::CollectStats(ref resp) => {
                                self.handle_collect_stats(&schema, resp, n.id).await?
                            }
                            piggyback_response::Info::CollectGroupDetail(ref resp) => {
                                self.handle_group_detail(&schema, resp).await?
                            }
                        }
                    }
                }
                Err(err) => {
                    self.liveness.init_node_if_first_seen(n.id);
                    warn!(node = n.id, target = ?n.addr, err = ?err, "send heartbeat error");
                }
            }
            self.heartbeat_queue
                .try_schedule(
                    HeartbeatTask { node_id: n.id },
                    last_heartbeat.add(self.cfg.heartbeat_interval()),
                )
                .await;
        }
        Ok(())
    }

    async fn try_send_heartbeat(
        &self,
        addr: String,
        piggybacks: &[PiggybackRequest],
        _timeout: Duration,
    ) -> Result<HeartbeatResponse> {
        let client = self.get_node_client(addr).await?;
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
        resp: &CollectStatsResponse,
        node_id: u64,
    ) -> Result<()> {
        if let Some(ns) = &resp.node_stats {
            if let Some(mut node) = schema.get_node(node_id).await? {
                let new_group_count = ns.group_count as u64;
                let new_leader_count = ns.leader_count as u64;
                let mut cap = node.capacity.take().unwrap();
                if new_group_count != cap.replica_count || new_leader_count != cap.leader_count {
                    cap.replica_count = new_group_count;
                    cap.leader_count = new_leader_count;
                    info!(
                        node = node_id,
                        replica_count = cap.replica_count,
                        leader_count = cap.leader_count,
                        "update node stats by heartbeat response",
                    );
                    node.capacity = Some(cap);
                    schema.update_node(node).await?;
                }
            }
        }
        Ok(())
    }

    async fn handle_group_detail(
        &self,
        schema: &Schema,
        resp: &CollectGroupDetailResponse,
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
            info!(
                group = desc.id,
                desc = ?desc,
                "update group_desc from heartbeat response"
            );
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
            info!(
                group = state.group_id,
                replica = state.replica_id,
                state = ?state,
                "attempt update replica_state from heartbeat response"
            );
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
