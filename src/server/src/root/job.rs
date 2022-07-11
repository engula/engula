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

use std::{collections::HashSet, sync::Arc, time::Duration};

use engula_api::server::v1::{
    watch_response::{update_event, UpdateEvent},
    *,
};
use engula_client::{NodeClient, RequestBatchBuilder};
use tokio::time;
use tracing::{info, warn};

use super::{
    allocator::{GroupAction, ReallocateReplica, ReallocateShard, ReplicaAction},
    Root, Schema,
};
use crate::{bootstrap::ROOT_GROUP_ID, Result};

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
                ReplicaAction::Migrate(action) => {
                    self.reallocate_replica(action).await?;
                }
            }
        }

        let shard_actions = self.alloc.compute_shard_action().await?;
        for shard_action in shard_actions {
            match shard_action {
                super::allocator::ShardAction::Noop => {}
                super::allocator::ShardAction::Migrate(action) => {
                    assert!(
                        action.source_group != ROOT_GROUP_ID
                            && action.target_group != ROOT_GROUP_ID
                    );
                    self.reallocate_shard(action).await?;
                }
            }
        }

        Ok(())
    }

    async fn reallocate_replica(&self, action: ReallocateReplica) -> Result<()> {
        let schema = self.schema()?;

        loop {
            if let Err(err) =
                Self::try_add_replica(schema.to_owned(), action.group, action.target_node.id).await
            {
                if is_retry_err(&err) {
                    time::sleep(Duration::from_secs(1)).await;
                    continue;
                } else {
                    return Err(err);
                }
            }
            break;
        }

        loop {
            if let Err(err) = Self::try_remove_replica(
                schema.to_owned(),
                action.group,
                action.source_replica,
                action.source_node,
            )
            .await
            {
                if is_retry_err(&err) {
                    time::sleep(Duration::from_secs(1)).await;
                    continue;
                } else {
                    return Err(err);
                }
            }
            break;
        }

        Ok(())
    }

    async fn try_add_replica(
        schema: Arc<Schema>,
        group_id: u64,
        target_node_id: u64,
    ) -> Result<()> {
        let (group, req_node) = Self::get_group_leader(schema.to_owned(), group_id).await?;

        // Create replica in target node.
        let new_replica = schema.next_replica_id().await?;
        let target_node = schema
            .get_node(target_node_id.to_owned())
            .await?
            .ok_or(crate::Error::GroupNotFound(group_id))?;
        let target_cli = NodeClient::connect((&target_node.addr).to_owned()).await?;
        target_cli
            .create_replica(
                new_replica,
                GroupDesc {
                    id: group_id,
                    ..Default::default()
                },
            )
            .await?;

        // Add new replica to group.
        let gl_client = NodeClient::connect(req_node.addr.to_owned()).await?;
        let batch = RequestBatchBuilder::new(req_node.id).add_replica(
            group.id,
            group.epoch,
            new_replica,
            target_node_id,
        );
        let resps = gl_client.batch_group_requests(batch.build()).await?;
        for resp in resps {
            if let Some(err) = resp.error {
                return Err(err.into());
            }
        }

        info!(
            "add replica: {} to group: {} in node: {}",
            new_replica, group.id, target_node_id
        );
        Ok(())
    }

    async fn try_remove_replica(
        schema: Arc<Schema>,
        group_id: u64,
        remove_replica: u64,
        source_node_id: u64,
    ) -> Result<()> {
        let (group, req_node) = Self::get_group_leader(schema.to_owned(), group_id).await?;

        let replica_state = schema
            .get_replica_state(group_id, remove_replica)
            .await?
            .ok_or(crate::Error::GroupNotFound(group.id))?;

        if replica_state.role == RaftRole::Leader.into() {
            if let Some(target_replica) = group.replicas.iter().find(|e| e.id != remove_replica) {
                info!(
                    "transfer group {} leader from {} to {}",
                    group.id, remove_replica, target_replica.id
                );
                let client = NodeClient::connect(req_node.addr.to_owned()).await?;
                let batch = RequestBatchBuilder::new(req_node.id).transfer_leader(
                    group.id,
                    group.epoch,
                    target_replica.id,
                );
                let resps = client.batch_group_requests(batch.build()).await?;
                for resp in resps {
                    if let Some(err) = resp.error {
                        return Err(err.into());
                    }
                }
                return Err(crate::Error::GroupNotFound(group_id));
            }
        }

        // Remove from leader desc.
        let client = NodeClient::connect(req_node.addr.to_owned()).await?;
        let batch = RequestBatchBuilder::new(req_node.id).remove_replica(
            group.id,
            group.epoch,
            remove_replica,
        );
        let resps = client.batch_group_requests(batch.build()).await?;
        for resp in resps {
            if let Some(err) = resp.error {
                return Err(err.into());
            }
        }

        info!(
            "remove replica: {} to group: {} from node: {}",
            remove_replica, group.id, source_node_id,
        );
        Ok(())
    }

    async fn get_group_leader(schema: Arc<Schema>, group_id: u64) -> Result<(GroupDesc, NodeDesc)> {
        let group = schema
            .get_group(group_id)
            .await?
            .ok_or(crate::Error::GroupNotFound(group_id))?;

        let mut group_leader = None;
        for replica in &group.replicas {
            if replica.role != ReplicaRole::Voter.into() {
                continue;
            }
            if let Some(rs) = schema.get_replica_state(group_id, replica.id).await? {
                if rs.role == RaftRole::Leader.into() {
                    group_leader = Some(replica);
                    break;
                }
            }
        }

        let group_leader = group_leader.ok_or(crate::Error::GroupNotFound(group_id))?;

        let leader_noder = schema
            .get_node(group_leader.node_id)
            .await?
            .ok_or(crate::Error::GroupNotFound(group_id))?;

        Ok((group.to_owned(), leader_noder))
    }

    async fn reallocate_shard(&self, action: ReallocateShard) -> Result<()> {
        let schema = self.schema()?;

        loop {
            if let Err(err) = Self::try_migrate_shard(
                schema.to_owned(),
                action.shard,
                action.source_group,
                action.target_group,
            )
            .await
            {
                if is_retry_err(&err) {
                    time::sleep(Duration::from_secs(1)).await;
                    continue;
                } else {
                    return Err(err);
                }
            }
            break;
        }

        Ok(())
    }

    async fn try_migrate_shard(
        schema: Arc<Schema>,
        shard: u64,
        src_group: u64,
        target_group: u64,
    ) -> Result<()> {
        let (target_group, target_node) =
            Self::get_group_leader(schema.to_owned(), target_group).await?;

        let (src_group, _) = Self::get_group_leader(schema.to_owned(), src_group).await?;

        let shard = src_group
            .shards
            .iter()
            .find(|s| s.id == shard)
            .ok_or(crate::Error::GroupNotFound(src_group.id))?;

        let client = NodeClient::connect(target_node.addr.to_owned()).await?;
        let batch = RequestBatchBuilder::new(target_node.id).accept_shard(
            target_group.id,
            target_group.epoch,
            src_group.id,
            src_group.epoch,
            shard,
        );
        let resps = client.batch_group_requests(batch.build()).await?;
        for resp in resps {
            if let Some(err) = resp.error {
                return Err(err.into());
            }
        }
        Ok(())
    }
}

pub(crate) fn is_retry_err(err: &crate::Error) -> bool {
    matches!(
        err,
        crate::Error::NotLeader(_, _)
            | crate::Error::GroupNotFound(_)
            | crate::Error::EpochNotMatch(_)
            | crate::Error::GroupNotReady(_)
    )
}
