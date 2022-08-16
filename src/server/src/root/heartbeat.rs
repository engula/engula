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

use std::{
    collections::{hash_map, HashMap, HashSet},
    ops::Add,
    sync::{Arc, Mutex},
    time::Duration,
    vec,
};

use engula_api::server::v1::{
    watch_response::{update_event, UpdateEvent},
    *,
};
use futures::future::join_all;
use tokio::time::Instant;
use tracing::{info, trace, warn};

use super::{HeartbeatTask, Root, Schema};
use crate::{
    bootstrap::ROOT_GROUP_ID,
    root::{metrics, schema::ReplicaNodes},
    Result,
};

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
            piggybacks.push(PiggybackRequest {
                info: Some(piggyback_request::Info::CollectScheduleState(
                    CollectScheduleStateRequest {},
                )),
            })
        }

        let resps = {
            let _timer = metrics::HEARTBEAT_NODES_RPC_DURATION_SECONDS.start_timer();
            metrics::HEARTBEAT_NODES_BATCH_SIZE.set(nodes.len() as i64);
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
            join_all(futs).await
        };

        let last_heartbeat = Instant::now();
        let mut heartbeat_tasks = Vec::new();
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
                            piggyback_response::Info::CollectScheduleState(ref resp) => {
                                self.handle_schedule_state(resp).await?
                            }
                        }
                    }
                }
                Err(err) => {
                    super::metrics::HEARTBEAT_TASK_FAIL_TOTAL
                        .with_label_values(&[&n.id.to_string()])
                        .inc();
                    self.liveness.init_node_if_first_seen(n.id);
                    warn!(node = n.id, target = ?n.addr, err = ?err, "send heartbeat error");
                }
            }
            heartbeat_tasks.push(HeartbeatTask { node_id: n.id })
        }
        self.heartbeat_queue
            .try_schedule(
                heartbeat_tasks,
                last_heartbeat.add(self.cfg.heartbeat_interval()),
            )
            .await;

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
                let _timer =
                    super::metrics::HEARTBEAT_HANDLE_NODE_STATS_DURATION_SECONDS.start_timer();
                let new_group_count = ns.group_count as u64;
                let new_leader_count = ns.leader_count as u64;
                let mut cap = node.capacity.take().unwrap();
                if new_group_count != cap.replica_count || new_leader_count != cap.leader_count {
                    super::metrics::HEARTBEAT_UPDATE_NODE_STATS_TOTAL.inc();
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
        let _timer = super::metrics::HEARTBEAT_HANDLE_GROUP_DETAIL_DURATION_SECONDS.start_timer();
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
            metrics::ROOT_UPDATE_GROUP_DESC_TOTAL.heartbeat.inc();
            info!(
                group = desc.id,
                desc = ?desc,
                "update group_desc from heartbeat response"
            );
            if desc.id == ROOT_GROUP_ID {
                self.heartbeat_queue
                    .try_schedule(
                        vec![HeartbeatTask {
                            node_id: self.current_node_id(),
                        }],
                        Instant::now(),
                    )
                    .await;
            }
            update_events.push(UpdateEvent {
                event: Some(update_event::Event::Group(desc.to_owned())),
            })
        }

        let mut changed_group_states = HashSet::new();
        for state in &resp.replica_states {
            if let Some(pre_state) = schema
                .get_replica_state(state.group_id, state.replica_id)
                .await?
            {
                if state.term < pre_state.term
                    || (state.term == pre_state.term && state.role == pre_state.role)
                {
                    continue;
                }
            }
            schema
                .update_group_replica(None, Some(state.to_owned()))
                .await?;
            metrics::ROOT_UPDATE_REPLICA_STATE_TOTAL.heartbeat.inc();
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

    async fn handle_schedule_state(&self, resp: &CollectScheduleStateResponse) -> Result<()> {
        self.delta_stats.handle_update(&resp.schedule_states, None);
        Ok(())
    }
}

struct ReplicaDelta {
    incoming: Vec<ReplicaDesc>,
    outgoing: Vec<ReplicaDesc>,
}

#[derive(Clone, Default)]
pub struct NodeDelta {
    pub replica_count: i64,
    // TODO: qps
}

#[derive(Default, Clone)]
pub struct DeltaStats {
    sched_inner: Arc<Mutex<ScheduleDeltaInner>>,
    job_inner: Arc<Mutex<JobInner>>,
}

#[derive(Default)]
struct ScheduleDeltaInner {
    raw_group_delta: HashMap<u64 /* group */, ReplicaDelta>,
    node_view: HashMap<u64 /* node */, NodeDelta>,
}

impl DeltaStats {
    fn handle_update(
        &self,
        state_updates: &[ScheduleState],
        job_updates: Option<HashMap<u64 /* node */, NodeDelta>>,
    ) {
        if !state_updates.is_empty() {
            let mut inner = self.sched_inner.lock().unwrap();
            inner.replace_state(state_updates);
            inner.rebuild_view();
        }
        if job_updates.is_some() {
            let mut inner = self.job_inner.lock().unwrap();
            inner.node_delta = job_updates.as_ref().unwrap().to_owned();
        }
    }

    pub fn get_node_delta(&self, node: u64) -> NodeDelta {
        let mut rs = NodeDelta::default();
        if let Some(sched_node_delta) = {
            let inner = self.sched_inner.lock().unwrap();
            inner.node_view.get(&node).map(ToOwned::to_owned)
        } {
            rs.replica_count += sched_node_delta.replica_count;
        }
        if let Some(job_node_delta) = {
            let inner = self.job_inner.lock().unwrap();
            inner.node_delta.get(&node).map(ToOwned::to_owned)
        } {
            rs.replica_count += job_node_delta.replica_count;
        }
        rs
    }

    pub fn reset(&self) {
        {
            let mut inner = self.sched_inner.lock().unwrap();
            inner.raw_group_delta.clear();
            inner.node_view.clear();
        }
        {
            let mut inner = self.job_inner.lock().unwrap();
            inner.node_delta.clear();
        }
    }
}

impl ScheduleDeltaInner {
    fn replace_state(&mut self, updates: &[ScheduleState]) {
        for state in updates {
            self.raw_group_delta.insert(
                state.group_id,
                ReplicaDelta {
                    incoming: state.incoming_replicas.to_owned(),
                    outgoing: state.outgoing_replicas.to_owned(),
                },
            );
        }
    }

    fn rebuild_view(&mut self) {
        let mut new_node_view: HashMap<u64, NodeDelta> = HashMap::new();
        for r in self.raw_group_delta.values() {
            for incoming in &r.incoming {
                match new_node_view.entry(incoming.node_id) {
                    hash_map::Entry::Occupied(mut ent) => ent.get_mut().replica_count += 1,
                    hash_map::Entry::Vacant(ent) => {
                        ent.insert(NodeDelta { replica_count: 1 });
                    }
                }
            }
            for outgoing in &r.outgoing {
                match new_node_view.entry(outgoing.node_id) {
                    hash_map::Entry::Occupied(mut ent) => ent.get_mut().replica_count -= 1,
                    hash_map::Entry::Vacant(ent) => {
                        ent.insert(NodeDelta { replica_count: -1 });
                    }
                }
            }
        }
        self.node_view = new_node_view;
    }
}

#[derive(Default)]
struct JobInner {
    node_delta: HashMap<u64 /* node_id */, NodeDelta>,
}
