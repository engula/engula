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

use std::sync::Arc;

use engula_api::server::v1::{GroupDesc, NodeDesc};

use self::{
    policy_leader_cnt::LeaderCountPolicy, policy_replica_cnt::ReplicaCountPolicy,
    policy_shard_cnt::ShardCountPolicy, source::NodeFilter,
};
use super::{metrics, OngoingStats, RootShared};
use crate::{constants::REPLICA_PER_GROUP, Result, RootConfig};

#[cfg(test)]
mod sim_test;

mod policy_leader_cnt;
mod policy_replica_cnt;
mod policy_shard_cnt;
mod source;

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
    Noop,
    Shed(TransferLeader),
}

#[derive(Debug, Clone)]
pub struct TransferLeader {
    pub group: u64,
    pub src_node: u64,
    pub src_replica: u64,
    pub target_node: u64,
    pub target_replica: u64,
}

#[derive(Clone, Debug)]
pub struct ReallocateReplica {
    pub group: u64,
    pub source_node: u64,
    pub source_replica: u64,
    pub target_node: NodeDesc,
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

#[derive(Clone)]
pub struct Allocator<T: AllocSource> {
    alloc_source: Arc<T>,
    ongoing_stats: Arc<OngoingStats>,
    config: RootConfig,
}

impl<T: AllocSource> Allocator<T> {
    pub fn new(alloc_source: Arc<T>, ongoing_stats: Arc<OngoingStats>, config: RootConfig) -> Self {
        Self {
            alloc_source,
            config,
            ongoing_stats,
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
            // group allocator start work after node_count > replicas_per_group.
            return Ok(GroupAction::Noop);
        }

        let desired_groups = self.desired_groups(self.config.replicas_per_group);
        Ok(match self.current_groups().cmp(&desired_groups) {
            std::cmp::Ordering::Less => {
                // it happend when:
                // - new join node
                // - increase cpu quota for exist node(e.g. via cgroup)
                // - increase replica_num configuration
                GroupAction::Add(desired_groups - self.current_groups())
            }
            std::cmp::Ordering::Greater => {
                // it happens when:
                //  - joined node exit
                //  - decrease cpu quota for exist node(e.g. via cgroup)
                //  - decrease replica_num configuration
                let want_remove = self.current_groups() - desired_groups;
                GroupAction::Remove(self.preferred_remove_groups(want_remove))
            }
            std::cmp::Ordering::Equal => GroupAction::Noop,
        })
    }

    /// Compute replica change action.
    pub async fn compute_replica_action(&self) -> Result<Vec<ReplicaAction>> {
        if !self.config.enable_replica_balance {
            return Ok(vec![]);
        }

        // compute_group_action refreshed.
        // self.alloc_source.refresh_all().await?;

        // TODO: try qps rebalance.

        // try replica-count rebalance.
        let actions =
            ReplicaCountPolicy::with(self.alloc_source.to_owned(), self.ongoing_stats.to_owned())
                .compute_balance()?;
        if !actions.is_empty() {
            return Ok(actions);
        }

        Ok(Vec::new())
    }

    pub async fn compute_shard_action(&self) -> Result<Vec<ShardAction>> {
        if !self.config.enable_shard_balance {
            return Ok(vec![]);
        }

        // always follow comput_replica_role_action() so no need refresh
        // self.alloc_source.refresh_all().await?;

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

    /// Allocate new replica in one group.
    pub async fn allocate_group_replica(
        &self,
        existing_replica_nodes: Vec<u64>,
        wanted_count: usize,
    ) -> Result<Vec<NodeDesc>> {
        self.alloc_source.refresh_all().await?;

        ReplicaCountPolicy::with(self.alloc_source.to_owned(), self.ongoing_stats.to_owned())
            .allocate_group_replica(existing_replica_nodes, wanted_count)
    }

    /// Find a group to place shard.
    pub async fn place_group_for_shard(&self, n: usize) -> Result<Vec<GroupDesc>> {
        self.alloc_source.refresh_all().await?;

        ShardCountPolicy::with(self.alloc_source.to_owned()).allocate_shard(n)
    }

    pub async fn compute_leader_action(&self) -> Result<Vec<LeaderAction>> {
        if !self.config.enable_leader_balance {
            return Ok(vec![]);
        }
        // self.alloc_source.refresh_all().await?;
        match LeaderCountPolicy::with(self.alloc_source.to_owned()).compute_balance()? {
            LeaderAction::Noop => {}
            e @ LeaderAction::Shed { .. } => return Ok(vec![e]),
        }
        Ok(Vec::new())
    }
}

impl<T: AllocSource> Allocator<T> {
    fn preferred_remove_groups(&self, want_remove: usize) -> Vec<u64> {
        // TODO:
        // 1 remove groups from unreachable nodes that indicated by NodeLiveness(they also need
        // repair replicas).
        // 2 remove groups from unmatched cpu-quota nodes.
        // 3. remove groups with lowest migration cost.
        self.alloc_source
            .nodes(NodeFilter::NotDecommissioned)
            .iter()
            .take(want_remove)
            .map(|n| n.id)
            .collect()
    }

    fn desired_groups(&self, replicas_per_group: usize) -> usize {
        let nodes = self.alloc_source.nodes(NodeFilter::NotDecommissioned);
        let total_nodes = nodes.len();
        let total_cpus = nodes
            .iter()
            .map(|n| n.capacity.as_ref().unwrap().cpu_nums)
            .fold(0_f64, |acc, x| acc + x);

        // We want only one worker per core serving a group, and we also want at least
        // one group per machine.
        std::cmp::max(
            (total_cpus / replicas_per_group as f64) as usize,
            total_nodes,
        )
    }

    fn current_groups(&self) -> usize {
        self.alloc_source.groups().len()
    }
}

// Allocate Group's replica between nodes.
impl<T: AllocSource> Allocator<T> {}

// Allocate Group leader replica.
impl<T: AllocSource> Allocator<T> {}
