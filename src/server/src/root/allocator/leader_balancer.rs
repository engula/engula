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

use engula_client::{GroupClient, RouterGroupState};
use tracing::info;

use super::*;
use crate::Result;

#[derive(Default)]
struct BalanceContext {
    count_delta: HashMap<u64 /* node */, f64 /* delta */>,
}

impl BalanceContext {
    fn get(&self, node: &u64) -> f64 {
        self.count_delta.get(node).unwrap_or(&0.0).to_owned()
    }

    fn update(&mut self, node: &u64, delta: f64) {
        let old = self.get(node);
        self.count_delta.insert(node.to_owned(), old + delta);
    }
}

#[derive(Debug)]
enum LeaderBalanceAction {
    AlreadyBalanced,
    TransferLeader {
        group: GroupDesc,
        leader_replica: ReplicaDesc,
        leader_term: u64,
        target_replica: ReplicaDesc,
    },
    MaybeBalanceReplica,
}

pub struct LeaderBalancer<T: AllocSource> {
    alloc_source: Arc<T>,
    shared: Arc<RootShared>,
    config: RootConfig,
}

impl<T: AllocSource> LeaderBalancer<T> {
    pub fn with(alloc_source: Arc<T>, shared: Arc<RootShared>, config: RootConfig) -> Self {
        Self {
            alloc_source,
            shared,
            config,
        }
    }

    pub async fn need_balance(&self) -> Result<bool> {
        if !self.config.enable_replica_balance {
            return Ok(false);
        }
        self.alloc_source.refresh_all().await?;
        let all_nodes = self.alloc_source.nodes(NodeFilter::Schedulable);
        let (_, min, max) = self.count_threshold(&all_nodes);
        let ctx = BalanceContext::default();
        for node in &all_nodes {
            let leader_cnt = self.leader_cnt(&ctx, node);
            if leader_cnt > max {
                let action = self.choose_leader_to_transfer(&ctx, node, &all_nodes, min)?;
                if !matches!(action, LeaderBalanceAction::AlreadyBalanced) {
                    metrics::RECONCILE_ALREADY_BALANCED_INFO
                        .node_leader_count
                        .set(0);
                    return Ok(true);
                }
            }
        }
        metrics::RECONCILE_ALREADY_BALANCED_INFO
            .node_leader_count
            .set(1);
        Ok(false)
    }

    pub async fn balance_leader(&self) -> Result<Vec<u64>> {
        if !self.config.enable_shard_balance {
            return Ok(vec![]);
        }

        self.alloc_source.refresh_all().await?;

        let mut maybe_move_replica_nodes = Vec::new();
        let all_nodes = self.alloc_source.nodes(NodeFilter::Schedulable);
        let (_, min, max) = self.count_threshold(&all_nodes);

        let mut ctx = BalanceContext::default();
        for node in &all_nodes {
            let mut leader_cnt = self.leader_cnt(&ctx, node);
            while leader_cnt > max {
                let balance_action = self.choose_leader_to_transfer(&ctx, node, &all_nodes, min)?;
                info!(
                    "balance leader on node: {}, leader: {} > max: {} - {:?}",
                    node.id, leader_cnt, max, balance_action
                );
                match balance_action {
                    LeaderBalanceAction::AlreadyBalanced => {
                        break;
                    }
                    LeaderBalanceAction::MaybeBalanceReplica => {
                        maybe_move_replica_nodes.push(node.id);
                        break;
                    }
                    LeaderBalanceAction::TransferLeader {
                        group,
                        leader_replica,
                        leader_term,
                        target_replica,
                    } => {
                        self.try_transfer_leader(
                            group,
                            (leader_replica.id, leader_term),
                            target_replica.id,
                        )
                        .await?;

                        leader_cnt -= 1.0;
                        ctx.update(&leader_replica.node_id, -1.0);
                        ctx.update(&target_replica.node_id, 1.0);
                    }
                }
            }
        }
        Ok(maybe_move_replica_nodes)
    }

    fn leader_cnt(&self, ctx: &BalanceContext, node: &NodeDesc) -> f64 {
        let delta = ctx.get(&node.id);
        node.capacity.as_ref().unwrap().leader_count as f64 + delta
    }

    fn choose_leader_to_transfer(
        &self,
        ctx: &BalanceContext,
        src_node: &NodeDesc,
        all_nodes: &[NodeDesc],
        min: f64,
    ) -> Result<LeaderBalanceAction> {
        let groups = self.alloc_source.groups();

        let mut not_suitable_target = false;
        let src_replicas = self.alloc_source.node_replicas(&src_node.id);
        for (leader_replica, group_id) in &src_replicas {
            if *group_id == ROOT_GROUP_ID {
                continue;
            }
            if let Some(replica_state) = self.alloc_source.replica_state(&leader_replica.id) {
                if !matches!(
                    RaftRole::from_i32(replica_state.role).unwrap(),
                    RaftRole::Leader
                ) {
                    continue;
                }

                let group = groups
                    .get(group_id)
                    .ok_or_else(|| crate::Error::GroupNotFound(group_id.to_owned()))?;
                let repl_cands = group
                    .replicas
                    .iter()
                    .filter(|r| {
                        if r.id == leader_replica.id {
                            return false;
                        }
                        if let Some(cand_node) = all_nodes.iter().find(|n| n.id == r.node_id) {
                            self.leader_cnt(ctx, cand_node) < min
                        } else {
                            false
                        }
                    })
                    .collect::<Vec<_>>();
                if repl_cands.is_empty() {
                    not_suitable_target = true;
                    continue;
                }
                let mut rng = thread_rng();
                let i = rng.gen_range(0..repl_cands.len());
                let target = repl_cands.get(i).unwrap();
                return Ok(LeaderBalanceAction::TransferLeader {
                    group: group.to_owned(),
                    leader_replica: leader_replica.to_owned(),
                    leader_term: replica_state.term,
                    target_replica: target.to_owned().to_owned(),
                });
            }
        }
        Ok(if not_suitable_target {
            LeaderBalanceAction::MaybeBalanceReplica
        } else {
            LeaderBalanceAction::AlreadyBalanced
        })
    }

    fn count_threshold(&self, cands: &[NodeDesc]) -> (f64, f64, f64) {
        const MIN_LEADER_DELTA: f64 = 0.5;
        let mean = self.mean_leader_count(cands);
        (mean, mean - MIN_LEADER_DELTA, mean + MIN_LEADER_DELTA)
    }

    fn mean_leader_count(&self, all_nodes: &[NodeDesc]) -> f64 {
        let total_leader = all_nodes
            .iter()
            .map(|n| n.capacity.as_ref().unwrap().leader_count as u64)
            .sum::<u64>() as f64;
        total_leader / (all_nodes.len() as f64)
    }

    async fn try_transfer_leader(
        &self,
        group: GroupDesc,
        leader_state: (u64 /* id */, u64 /* term */),
        target_replica: u64,
    ) -> Result<()> {
        let group_state = RouterGroupState {
            id: group.id,
            epoch: group.epoch,
            leader_state: Some(leader_state),
            replicas: group
                .replicas
                .iter()
                .map(|g| (g.id, g.to_owned()))
                .collect::<HashMap<_, _>>(),
        };
        let mut group_client = GroupClient::new(
            group_state,
            self.shared.provider.router.clone(),
            self.shared.provider.conn_manager.clone(),
        );
        group_client.transfer_leader(target_replica).await?;
        Ok(())
    }
}
