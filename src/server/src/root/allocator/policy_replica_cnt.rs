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
    cmp::Ordering,
    collections::{HashMap, HashSet},
    sync::Arc,
};

use engula_api::server::v1::{NodeDesc, ReplicaDesc};
use tracing::trace;

use super::{source::NodeFilter, *};
use crate::{bootstrap::ROOT_GROUP_ID, root::OngoingStats, Result};

pub struct ReplicaCountPolicy<T: AllocSource> {
    alloc_source: Arc<T>,
    ongoing_stats: Arc<OngoingStats>,
}

impl<T: AllocSource> ReplicaCountPolicy<T> {
    pub fn with(alloc_source: Arc<T>, ongoing_stats: Arc<OngoingStats>) -> Self {
        Self {
            alloc_source,
            ongoing_stats,
        }
    }

    pub fn allocate_group_replica(
        &self,
        existing_replica_nodes: Vec<u64>,
        wanted_count: usize,
    ) -> Result<Vec<NodeDesc>> {
        let mut candidate_nodes = self.alloc_source.nodes(NodeFilter::Schedulable);

        // skip the nodes already have group replicas.
        candidate_nodes.retain(|n| !existing_replica_nodes.iter().any(|rn| *rn == n.id));

        // sort by alloc score
        candidate_nodes.sort_by(|n1, n2| {
            self.node_alloc_score(n2)
                .partial_cmp(&self.node_alloc_score(n1))
                .unwrap()
        });

        Ok(candidate_nodes.into_iter().take(wanted_count).collect())
    }

    pub fn compute_balance(&self) -> Result<Vec<ReplicaAction>> {
        let mean_cnt = self.mean_replica_count(NodeFilter::Schedulable);
        let candidate_nodes = self.alloc_source.nodes(NodeFilter::Schedulable);

        let ranked_candidates = self.rank_node_for_balance(candidate_nodes, mean_cnt);
        trace!(
            scored_nodes = ?ranked_candidates.iter().map(|(n, s)| format!("{}-{}({:?})", n.id, self.node_replica_count(n), s)).collect::<Vec<_>>(),
            mean = mean_cnt,
            "node ranked by replica count",
        );
        for (src_node, status) in &ranked_candidates {
            if *status != BalanceStatus::Overfull {
                break;
            }
            if let Some(action) = self.rebalance_target(src_node, &ranked_candidates, mean_cnt) {
                return Ok(vec![action]);
            }
        }

        Ok(Vec::new())
    }

    fn rebalance_target(
        &self,
        src: &NodeDesc,
        ranked_nodes: &[(NodeDesc, BalanceStatus)],
        mean: f64,
    ) -> Option<ReplicaAction> {
        let mut groups = self
            .alloc_source
            .groups()
            .into_iter()
            .map(|(group, desc)| {
                (
                    group,
                    desc.replicas
                        .iter()
                        .map(|r| r.node_id)
                        .collect::<HashSet<u64>>(),
                )
            })
            .collect::<HashMap<_, _>>();

        let replica_states = self.alloc_source.replica_states();
        for replica_state in replica_states {
            if let Some(g) = groups.get_mut(&replica_state.group_id) {
                g.insert(replica_state.node_id);
            }
        }

        for (target, state) in ranked_nodes.iter().rev() {
            if *state != BalanceStatus::Underfull {
                break;
            }
            let sim_count = (self.node_replica_count(target) + 1) as f64;
            if Self::node_balance_state(sim_count, mean) == BalanceStatus::Overfull {
                continue;
            }
            let (source_replica, group) = self.preferred_remove_replica(src, target, &groups)?;
            return Some(ReplicaAction::Migrate(ReallocateReplica {
                group,
                source_node: source_replica.node_id,
                source_replica: source_replica.id,
                target_node: target.to_owned(),
            }));
        }
        None
    }

    fn preferred_remove_replica(
        &self,
        src: &NodeDesc,
        target: &NodeDesc,
        group_nodes: &HashMap<u64, HashSet<u64>>,
    ) -> Option<(ReplicaDesc, u64)> {
        // TODO: sort & rank replica
        self.alloc_source
            .node_replicas(&src.id)
            .into_iter()
            .find(|(_, g)| {
                if *g == ROOT_GROUP_ID {
                    return false;
                }
                if let Some(exist_nodes) = group_nodes.get(g) {
                    if exist_nodes.len() < REPLICA_PER_GROUP {
                        return false;
                    }
                    if !exist_nodes.contains(&target.id) {
                        return true;
                    }
                }
                false
            })
    }

    fn mean_replica_count(&self, filter: NodeFilter) -> f64 {
        let nodes = self.alloc_source.nodes(filter);
        let total_replicas = nodes
            .iter()
            .map(|n| self.node_replica_count(n))
            .sum::<u64>() as f64;
        total_replicas / (nodes.len() as f64)
    }

    fn rank_node_for_balance(
        &self,
        ns: Vec<NodeDesc>,
        mean_cnt: f64,
    ) -> Vec<(NodeDesc, BalanceStatus)> {
        let mut with_status = ns
            .into_iter()
            .map(|n| {
                let replica_num = self.node_replica_count(&n) as f64;
                let s = Self::node_balance_state(replica_num, mean_cnt);
                (n, s)
            })
            .collect::<Vec<(NodeDesc, BalanceStatus)>>();
        with_status.sort_by(|n1, n2| {
            if (n2.1 == BalanceStatus::Overfull) && (n1.1 != BalanceStatus::Overfull) {
                return Ordering::Greater;
            }
            if (n2.1 == BalanceStatus::Underfull) && (n1.1 != BalanceStatus::Underfull) {
                return Ordering::Less;
            }
            let n2_cnt = self.node_replica_count(&n2.0);
            let n1_cnt = self.node_replica_count(&n1.0);
            n2_cnt.cmp(&n1_cnt)
        });
        with_status
    }

    fn node_balance_state(replica_num: f64, mean: f64) -> BalanceStatus {
        const THRESHOLD_FRACTION: f64 = 0.05;
        const MIN_RANGE_DELTA: f64 = 2.0;
        let delta = f64::max(mean as f64 * THRESHOLD_FRACTION, MIN_RANGE_DELTA);
        if replica_num > mean + delta {
            return BalanceStatus::Overfull;
        }
        if replica_num < mean - delta {
            return BalanceStatus::Underfull;
        }
        BalanceStatus::Balanced
    }

    fn node_alloc_score(&self, n: &NodeDesc) -> f64 {
        // TODO: add more rule to calculate score.
        -(self.node_replica_count(n) as f64)
    }

    fn node_replica_count(&self, n: &NodeDesc) -> u64 {
        let mut cnt = n.capacity.as_ref().unwrap().replica_count as i64;
        let delta = self.ongoing_stats.get_node_delta(n.id);
        cnt += delta.replica_count;
        if cnt < 0 {
            cnt = 0;
        }
        cnt as u64
    }
}
