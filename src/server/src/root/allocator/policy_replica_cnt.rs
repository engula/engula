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

use std::{cmp::Ordering, sync::Arc};

use engula_api::server::v1::{NodeDesc, ReplicaDesc};

use super::*;
use crate::{bootstrap::ROOT_GROUP_ID, Result};

pub struct ReplicaCountPolicy<T: AllocSource> {
    alloc_source: Arc<T>,
}

#[derive(PartialEq, Eq)]
enum BalanceStatus {
    Overfull,
    Balanced,
    Underfull,
}

impl<T: AllocSource> ReplicaCountPolicy<T> {
    pub fn with(alloc_source: Arc<T>) -> Self {
        Self { alloc_source }
    }

    pub fn allocate_group_replica(
        &self,
        existing_replicas: Vec<ReplicaDesc>,
        wanted_count: usize,
    ) -> Result<Vec<NodeDesc>> {
        let mut candidate_nodes = self.alloc_source.nodes();

        // skip the nodes already have group replicas.
        candidate_nodes.retain(|n| !existing_replicas.iter().any(|r| r.node_id == n.id));

        // sort by alloc score
        candidate_nodes.sort_by(|n1, n2| {
            Self::node_alloc_score(n2)
                .partial_cmp(&Self::node_alloc_score(n1))
                .unwrap()
        });

        Ok(candidate_nodes.into_iter().take(wanted_count).collect())
    }

    pub fn compute_balance(&self) -> Result<Vec<ReplicaAction>> {
        let mean_cnt = self.mean_replica_count();
        let candidate_nodes = self.alloc_source.nodes();

        let ranked_candidates = Self::rank_node_for_balance(candidate_nodes, mean_cnt);
        for (src_node, status) in &ranked_candidates {
            if *status != BalanceStatus::Overfull {
                break;
            }
            if let Some(action) = self.rebalance_target(src_node, &ranked_candidates) {
                return Ok(vec![action]);
            }
        }

        Ok(Vec::new())
    }

    fn rebalance_target(
        &self,
        src: &NodeDesc,
        ranked_nodes: &[(NodeDesc, BalanceStatus)],
    ) -> Option<ReplicaAction> {
        let mean = self.mean_replica_count();
        let (source_replica, group) = self.preferred_remove_replica(src)?;
        for (target, state) in ranked_nodes.iter().rev() {
            if *state != BalanceStatus::Underfull {
                break;
            }
            let sim_count = (target.capacity.as_ref().unwrap().replica_count + 1) as f64;
            if Self::node_balance_state(sim_count, mean) == BalanceStatus::Overfull {
                continue;
            }
            return Some(ReplicaAction::Migrate(ReallocateReplica {
                group,
                source_node: source_replica.node_id,
                source_replica: source_replica.id,
                target_node: target.to_owned(),
            }));
        }
        None
    }

    fn preferred_remove_replica(&self, n: &NodeDesc) -> Option<(ReplicaDesc, u64)> {
        let replicas = self.alloc_source.node_replicas(&n.id);
        // TODO: ranking replica and choose the preferred one.
        for r in replicas {
            if r.1 != ROOT_GROUP_ID {
                return Some(r);
            }
        }
        None
    }

    fn mean_replica_count(&self) -> f64 {
        let nodes = self.alloc_source.nodes();
        let total_replicas = nodes
            .iter()
            .map(|n| n.capacity.as_ref().unwrap().replica_count)
            .sum::<u64>() as f64;
        total_replicas / (nodes.len() as f64)
    }

    fn rank_node_for_balance(ns: Vec<NodeDesc>, mean_cnt: f64) -> Vec<(NodeDesc, BalanceStatus)> {
        let mut with_status = ns
            .into_iter()
            .map(|n| {
                let replica_num = n.capacity.as_ref().unwrap().replica_count as f64;
                let s = Self::node_balance_state(replica_num, mean_cnt);
                (n, s)
            })
            .collect::<Vec<(NodeDesc, BalanceStatus)>>();
        with_status.sort_by(|n1, n2| {
            if (n2.1 == BalanceStatus::Overfull) && (n1.1 != BalanceStatus::Overfull) {
                return Ordering::Greater;
            }
            if (n1.1 == BalanceStatus::Underfull) && (n2.1 != BalanceStatus::Underfull) {
                return Ordering::Less;
            }
            return n2
                .0
                .capacity
                .as_ref()
                .unwrap()
                .replica_count
                .cmp(&n1.0.capacity.as_ref().unwrap().replica_count);
        });
        with_status
    }

    fn node_balance_state(replica_num: f64, mean: f64) -> BalanceStatus {
        const THRESHOLD_FRACTION: f64 = 0.05;
        const MIN_RANGE_DELTA: f64 = 2.0;
        let delta = f64::min(mean as f64 * THRESHOLD_FRACTION, MIN_RANGE_DELTA);
        if replica_num > mean + delta {
            return BalanceStatus::Overfull;
        }
        if replica_num < mean - delta {
            return BalanceStatus::Underfull;
        }
        BalanceStatus::Balanced
    }

    fn node_alloc_score(n: &NodeDesc) -> f64 {
        // TODO: add more rule to calculate score.
        -(n.capacity.as_ref().unwrap().replica_count as f64)
    }
}
