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

use engula_api::server::v1::{GroupDesc, NodeDesc, ReplicaDesc};
use rand::seq::SliceRandom;

use self::policy_replica_cnt::ReplicaCountPolicy;
use super::RootShared;
use crate::Result;

#[cfg(test)]
mod sim_test;

mod policy_replica_cnt;
mod source;

pub use source::{AllocSource, SysAllocSource};

pub enum GroupAction {
    Noop,
    Add(usize),
    Remove(Vec<u64>),
}

#[allow(dead_code)]
pub enum ReplicaAction {
    Noop,
    Migrate(MigrateAction),
}

#[allow(dead_code)]
pub struct MigrateAction {
    source_replica: u64,
    target_node: NodeDesc,
}

#[derive(Clone)]
pub struct Allocator<T: AllocSource> {
    replicas_per_group: usize,
    alloc_source: Arc<T>,
}

impl<T: AllocSource> Allocator<T> {
    pub fn new(alloc_source: Arc<T>, replicas_per_group: usize) -> Self {
        Self {
            alloc_source,
            replicas_per_group,
        }
    }

    /// Compute group change action.
    pub async fn compute_group_action(&self) -> Result<GroupAction> {
        self.alloc_source.refresh_all().await?;

        if self.alloc_source.nodes().len() < self.replicas_per_group {
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

    /// Compute replica change action.
    pub async fn compute_replica_action(&self) -> Result<Vec<ReplicaAction>> {
        self.alloc_source.refresh_all().await?;

        // maybe TODO repaire group replica.

        // TODO: try qps rebalance.

        // try replica-count rebalance.
        let actions = ReplicaCountPolicy::with(self.alloc_source.to_owned()).compute_balance()?;
        if !actions.is_empty() {
            return Ok(actions);
        }

        Ok(Vec::new())
    }

    /// Allocate new replica in one group.
    pub async fn allocate_group_replica(
        &self,
        existing_replicas: Vec<ReplicaDesc>,
        wanted_count: usize,
    ) -> Result<Vec<NodeDesc>> {
        self.alloc_source.refresh_all().await?;

        ReplicaCountPolicy::with(self.alloc_source.to_owned())
            .allocate_group_replica(existing_replicas, wanted_count)
    }

    /// Find a group to place shard.
    pub async fn place_group_for_shard(
        &self, /* , _shard_usage: &ShardDesc */
    ) -> Result<Option<GroupDesc>> {
        self.alloc_source.refresh_all().await?;
        let mut groups = self.alloc_source.groups();
        if groups.is_empty() {
            return Ok(None);
        }
        Ok(if groups.iter().any(|g| g.capacity.is_none()) {
            groups
                .choose(&mut rand::thread_rng())
                .map(ToOwned::to_owned)
        } else {
            groups.sort_by(|g1, g2| {
                g1.capacity
                    .as_ref()
                    .unwrap()
                    .shard_count
                    .cmp(&g2.capacity.as_ref().unwrap().shard_count)
            });
            groups.get(0).map(ToOwned::to_owned)
        })
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
            .nodes()
            .iter()
            .take(want_remove)
            .map(|n| n.id)
            .collect()
    }

    fn desired_groups(&self) -> usize {
        let total_cpus = self
            .alloc_source
            .nodes()
            .iter()
            .map(|n| n.capacity.as_ref().unwrap().cpu_nums)
            .fold(0_f64, |acc, x| acc + x);
        (total_cpus / self.replicas_per_group as f64).ceil() as usize
    }

    fn current_groups(&self) -> usize {
        self.alloc_source.groups().len()
    }
}

// Allocate Group's replica between nodes.
impl<T: AllocSource> Allocator<T> {}

// Allocate Group leader replica.
impl<T: AllocSource> Allocator<T> {}
