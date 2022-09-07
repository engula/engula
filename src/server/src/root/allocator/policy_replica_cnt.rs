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

use tracing::info;

use super::{node_balancer::*, *};
use crate::root::metrics::ROOT_NODE_REPLICA_MEAN_COUNT;

#[derive(Clone, Copy)]
pub struct ReplicaCountPolicy {
    pub goal: BalanceGoal,
}

impl Default for ReplicaCountPolicy {
    fn default() -> Self {
        Self {
            goal: BalanceGoal::ReplicaConvergence,
        }
    }
}

impl BalancePolicy for ReplicaCountPolicy {
    fn should_balance(&self, rep: &PotentialReplacement) -> bool {
        if rep.candidates.is_empty() {
            return false;
        }
        let (mean, min, max) = self.count_threshold(&rep.candidates);
        ROOT_NODE_REPLICA_MEAN_COUNT.set(mean);
        let cnt = rep.existing.balance_value;
        if cnt > max {
            // balance if over max a lot.
            info!(
                "should balance for overfull, node: {}, {cnt} > {max}",
                rep.existing.node.id
            );
            return true;
        }
        if cnt > mean {
            // balance if over mean and others is underfull.
            for c in &rep.candidates {
                let cand_cnt = c.balance_value;
                if cand_cnt < min {
                    info!(
                        "should balance for better-fit node: {}, cand: {}, {cand_cnt} < {min}",
                        rep.existing.node.id, c.node.id
                    );
                    return true;
                }
            }
        }
        false
    }

    fn balance_score(
        &self,
        node: &NodeCandidate,
        cands: &[NodeCandidate],
        from: bool,
    ) -> (f64, f64) {
        let current = node.balance_value;
        let (mean, min, max) = self.count_threshold(cands);
        let mut balance_score = 0.0;
        if current < min {
            balance_score = 1.0;
        }
        if current > max {
            balance_score = -1.0;
        }
        let new_val = if from { current - 1.0 } else { current + 1.0 };
        let converges_score = if f64::abs(new_val - mean) < f64::abs(current - mean) {
            if from {
                0.0
            } else {
                1.0
            }
        } else if from {
            1.0
        } else {
            0.0
        };
        info!("DEBUG: type: {:?}, min: {min}, max: {max}, mean: {mean}, current: {current}, bs: {balance_score}, cs: {converges_score}, src: {:?}, target: {:?}", self.goal(), node, cands);
        (balance_score, converges_score)
    }

    fn balance_value(&self, _ongoing_stats: Arc<OngoingStats>, n: &NodeDesc) -> u64 {
        let cnt = n.capacity.as_ref().unwrap().replica_count as i64;
        // let delta = ongoing_stats.get_node_delta(n.id);
        // cnt += delta.replica_count;
        // if cnt < 0 {
        // cnt = 0;
        // }
        cnt as u64
    }

    fn goal(&self) -> BalanceGoal {
        self.goal
    }
}

impl ReplicaCountPolicy {
    fn count_threshold(&self, cands: &[NodeCandidate]) -> (f64, f64, f64) {
        const THRESHOLD_FRACTION: f64 = 0.05;
        const MIN_RANGE_DELTA: f64 = 2.0;
        let mean = self.mean_candidate_replica_count(cands);
        let delta = f64::max(mean as f64 * THRESHOLD_FRACTION, MIN_RANGE_DELTA);
        (mean, mean - delta, mean + delta)
    }

    fn mean_candidate_replica_count(&self, cands: &[NodeCandidate]) -> f64 {
        let total = cands
            .iter()
            .filter(|c| c.node.capacity.is_some())
            .fold(0f64, |t, c| t + c.balance_value) as f64;
        let cnt = cands.iter().filter(|c| c.node.capacity.is_some()).count() as f64;
        total / cnt
    }
}
