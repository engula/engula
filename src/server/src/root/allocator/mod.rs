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

use std::{cmp::Ordering, collections::HashMap, sync::Arc, time::Duration};

use engula_api::server::v1::*;
use rand::{thread_rng, Rng};
use serde::{Deserialize, Serialize};

use self::{node_balancer::*, policy_shard_cnt::ShardCountPolicy, source::NodeFilter};
use super::{metrics, OngoingStats, RootShared};
use crate::{
    bootstrap::{REPLICA_PER_GROUP, ROOT_GROUP_ID},
    Result,
};

#[cfg(test)]
mod sim_test;

mod node_balancer;
mod policy_leader_cnt;
mod policy_replica_cnt;
mod policy_shard_cnt;
mod source;

pub use node_balancer::BalancePolicy;
pub use policy_leader_cnt::LeaderCountPolicy;
pub use policy_replica_cnt::ReplicaCountPolicy;
pub use source::{AllocSource, SysAllocSource};

#[derive(Clone, Debug)]
pub enum ReplicaRoleAction {
    Replica(ReplicaAction),
    Leader(LeaderAction),
}

#[derive(Clone, Debug)]
pub enum GroupAction {
    Noop,
    Add(usize),
    Remove(Vec<u64>),
}

#[derive(Clone, Debug)]
pub enum ReplicaAction {
    Migrate(ReallocateReplica),
}

#[derive(Clone, Debug)]
pub enum ShardAction {
    Migrate(ReallocateShard),
}

#[derive(Clone, Debug)]
pub enum LeaderAction {
    Shed(TransferLeader),
}

#[derive(Debug, Clone)]
pub struct TransferLeader {
    pub group: u64,
    pub epoch: u64,
    pub src_node: u64,
    pub target_node: u64,
}

#[derive(Clone, Debug)]
pub struct ReallocateReplica {
    pub group: u64,
    pub epoch: u64,
    pub source_node: u64,
    pub dest_node: u64,
}

#[derive(Clone, Debug)]
pub struct ReallocateShard {
    pub shard: u64,
    pub source_group: u64,
    pub target_group: u64,
}

#[derive(PartialEq, Eq, Debug)]
enum BalanceStatus {
    Overfull,
    Balanced,
    Underfull,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RootConfig {
    pub replicas_per_group: usize,
    pub enable_group_balance: bool,
    pub enable_replica_balance: bool,
    pub enable_shard_balance: bool,
    pub liveness_threshold_sec: u64,
    pub heartbeat_timeout_sec: u64,
    pub schedule_interval_sec: u64,
    pub max_create_group_retry_before_rollback: u64,
}

impl Default for RootConfig {
    fn default() -> Self {
        Self {
            replicas_per_group: REPLICA_PER_GROUP,
            enable_group_balance: true,
            enable_replica_balance: true,
            enable_shard_balance: true,
            liveness_threshold_sec: 30,
            heartbeat_timeout_sec: 4,
            schedule_interval_sec: 1,
            max_create_group_retry_before_rollback: 10,
        }
    }
}

impl RootConfig {
    pub fn heartbeat_interval(&self) -> Duration {
        Duration::from_secs(self.liveness_threshold_sec - self.heartbeat_timeout_sec)
    }
}

pub struct NodeBalanceAction {
    pub group_id: u64,
    pub epoch: u64,
    pub source_node: u64,
    pub dest_node: u64,
}

#[derive(Clone)]
pub struct Allocator<T: AllocSource> {
    alloc_source: Arc<T>,
    ongoing_stats: Arc<OngoingStats>,
    config: RootConfig,
    balancer: node_balancer::NodeBalancer<T>,
}

impl<T: AllocSource> Allocator<T> {
    pub fn new(alloc_source: Arc<T>, ongoing_stats: Arc<OngoingStats>, config: RootConfig) -> Self {
        let balancer = node_balancer::NodeBalancer::new(
            alloc_source.to_owned(),
            ongoing_stats.to_owned(),
            config.to_owned(),
        );
        Self {
            alloc_source,
            config,
            ongoing_stats,
            balancer,
        }
    }

    pub fn replicas_per_group(&self) -> usize {
        self.config.replicas_per_group
    }

    /// Compute group change action.
    pub async fn compute_group_action(&self) -> Result<GroupAction> {
        if !self.config.enable_group_balance {
            return Ok(GroupAction::Noop);
        }

        self.alloc_source.refresh_all().await?;

        if self.alloc_source.nodes(NodeFilter::NotDecommissioned).len()
            < self.config.replicas_per_group
        {
            // group alloctor start work after node_count > replicas_per_group.
            return Ok(GroupAction::Noop);
        }

        Ok(match self.current_groups().cmp(&self.desired_groups()) {
            std::cmp::Ordering::Less => {
                // it happend when:
                // - new join node
                // - increase cpu quota for exist node(e.g. via cgroup)
                // - increate replica_num configuration
                GroupAction::Add(self.desired_groups() - self.current_groups())
            }
            std::cmp::Ordering::Greater => {
                // it happens when:
                //  - joined node exit
                //  - decrease cpu quota for exist node(e.g. via cgroup)
                //  - decrease replica_num configuration
                let want_remove = self.current_groups() - self.desired_groups();
                GroupAction::Remove(self.preferred_remove_groups(want_remove))
            }
            std::cmp::Ordering::Equal => GroupAction::Noop,
        })
    }

    pub async fn compute_shard_action(&self) -> Result<Vec<ShardAction>> {
        if !self.config.enable_shard_balance {
            return Ok(vec![]);
        }

        self.alloc_source.refresh_all().await?;

        if self.alloc_source.nodes(NodeFilter::All).len() >= self.config.replicas_per_group {
            let actions = ShardCountPolicy::with(self.alloc_source.to_owned()).compute_balance()?;
            if !actions.is_empty() {
                metrics::RECONCILE_ALREADY_BALANCED_INFO
                    .group_shard_count
                    .set(0);
                return Ok(actions);
            }
        }
        metrics::RECONCILE_ALREADY_BALANCED_INFO
            .group_shard_count
            .set(1);
        Ok(Vec::new())
    }

    pub async fn compute_balance_action(
        &self,
        policy: &(dyn BalancePolicy + Send + Sync + 'static),
    ) -> Result<Vec<NodeBalanceAction>> {
        self.balancer.compute_balance_action(policy).await
    }

    /// Allocate new replica in one group.
    pub async fn allocate_group_replica(
        &self,
        existing_replica_nodes: Vec<u64>,
        wanted_count: usize,
    ) -> Result<Vec<NodeDesc>> {
        self.alloc_source.refresh_all().await?;

        let mut candidate_nodes = self.alloc_source.nodes(NodeFilter::Schedulable);

        // skip the nodes already have group replicas.
        candidate_nodes.retain(|n| !existing_replica_nodes.iter().any(|rn| *rn == n.id));

        let policy = ReplicaCountPolicy::default();

        let candidate_nodes = candidate_nodes
            .into_iter()
            .map(|node| NodeCandidate {
                node: node.to_owned(),
                disk_full: check_node_full(&node),
                balance_value: policy.balance_value(self.ongoing_stats.to_owned(), &node) as f64,
                ..Default::default()
            })
            .collect::<Vec<_>>();

        let mut candidate_nodes = candidate_nodes
            .iter()
            .map(|n| {
                let (balance_score, converges_score) =
                    policy.balance_score(n, &candidate_nodes, false);
                NodeCandidate {
                    node: n.node.to_owned(),
                    disk_full: n.disk_full,
                    balance_value: n.balance_value,
                    balance_score,
                    converges_score,
                }
            })
            .collect::<Vec<_>>();

        candidate_nodes.sort_by_key(|c| std::cmp::Reverse(c.to_owned()));

        Ok(candidate_nodes
            .into_iter()
            .take(wanted_count)
            .map(|n| n.node)
            .collect())
    }

    /// Find a group to place shard.
    pub async fn place_group_for_shard(&self, n: usize) -> Result<Vec<GroupDesc>> {
        self.alloc_source.refresh_all().await?;

        ShardCountPolicy::with(self.alloc_source.to_owned()).allocate_shard(n)
    }
}

impl<T: AllocSource> Allocator<T> {
    fn preferred_remove_groups(&self, want_remove: usize) -> Vec<u64> {
        // TODO:
        // 1 remove groups from unreachable nodes that indicated by NodeLiveness(they also need
        // repair replicas).
        // 2 remove groups from unmatch cpu-quota nodes.
        // 3. remove groups with lowest migration cost.
        self.alloc_source
            .nodes(NodeFilter::NotDecommissioned)
            .iter()
            .take(want_remove)
            .map(|n| n.id)
            .collect()
    }

    fn desired_groups(&self) -> usize {
        let total_cpus = self
            .alloc_source
            .nodes(NodeFilter::NotDecommissioned)
            .iter()
            .map(|n| n.capacity.as_ref().unwrap().cpu_nums)
            .fold(0_f64, |acc, x| acc + x);
        total_cpus as usize
    }

    fn current_groups(&self) -> usize {
        self.alloc_source.groups().len()
    }
}

fn almost_same_score(s1: f64, s2: f64) -> bool {
    f64::abs(s1 - s2) < 1e-10
}

fn best(replace_candidates: &[NodeCandidate]) -> Vec<NodeCandidate> {
    let valid_candidates = replace_candidates
        .iter()
        .take_while(|cand| !cand.disk_full)
        .cloned()
        .collect::<Vec<_>>();

    if valid_candidates.len() <= 1 {
        return valid_candidates;
    }

    let mut pre_cand: Option<NodeCandidate> = None;
    let best_candidates = valid_candidates
        .iter()
        .take_while(|cand| {
            if let Some(pre) = &pre_cand {
                if !pre.almost_same(cand) {
                    return false;
                }
            }
            pre_cand = Some(cand.to_owned().to_owned());
            true
        })
        .cloned()
        .collect::<Vec<_>>();

    best_candidates
}

fn select_good(cands: &[NodeCandidate]) -> Option<NodeCandidate> {
    let mut cands = cands.to_owned();
    cands.sort_by_key(|w| std::cmp::Reverse(w.to_owned()));
    let bests = best(&cands);
    if bests.is_empty() {
        return None;
    }
    if bests.len() == 1 {
        return Some(bests.first().unwrap().to_owned());
    }

    let mut rng = thread_rng();
    let mut best = bests.first().unwrap();
    for _ in 0..2 {
        let i = rng.gen_range(0..bests.len());
        if best.worse(bests.get(i).unwrap()) {
            best = bests.get(i).unwrap();
        }
    }
    Some(best.to_owned())
}

fn worst(candidates: &[NodeCandidate]) -> Vec<NodeCandidate> {
    if candidates.len() <= 1 {
        return candidates.to_owned();
    }
    {
        let full_candidates = candidates
            .iter()
            .rev()
            .take_while(|c| c.disk_full)
            .collect::<Vec<_>>();
        if !full_candidates.is_empty() {
            // choose replica from full node if exist.
            full_candidates
        } else {
            // choose replicas from lowest score.
            let mut pre_cand: Option<NodeCandidate> = None;
            candidates
                .iter()
                .rev()
                .take_while(|cand| {
                    if let Some(pre) = &pre_cand {
                        if !pre.almost_same(cand) {
                            return false;
                        }
                    }
                    pre_cand = Some(cand.to_owned().to_owned());
                    true
                })
                .collect::<Vec<_>>()
        }
    }
    .into_iter()
    .rev()
    .cloned()
    .collect::<Vec<_>>()
}

fn select_bad(candidates: &[NodeCandidate]) -> Option<NodeCandidate> {
    let mut cands = candidates.to_owned();
    cands.sort_by_key(|w| std::cmp::Reverse(w.to_owned()));
    let candidates = worst(&cands);
    if candidates.is_empty() {
        return None;
    }
    if candidates.len() == 1 {
        return Some(candidates.first().unwrap().to_owned());
    }
    let mut rng = thread_rng();
    let mut bad = candidates.first().unwrap();
    for _ in 0..2 {
        let i = rng.gen_range(0..candidates.len());
        if !bad.worse(candidates.get(i).unwrap()) {
            bad = candidates.get(i).unwrap();
        }
    }
    Some(bad.to_owned())
}

fn check_node_full(_n: &NodeDesc) -> bool {
    false // TODO:...
}

// Allocate Group's replica between nodes.
impl<T: AllocSource> Allocator<T> {}

// Allocate Group leader replica.
impl<T: AllocSource> Allocator<T> {}
