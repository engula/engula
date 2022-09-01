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

use super::{node_balancer::*, policy_replica_cnt::ReplicaCountPolicy, *};
use crate::root::OngoingStats;

#[derive(Clone, Copy, Default)]
pub struct LeaderCountPolicy {
    count_policy: ReplicaCountPolicy,
}

impl BalancePolicy for LeaderCountPolicy {
    fn balance_value(&self, _ongoing_stats: Arc<OngoingStats>, n: &NodeDesc) -> u64 {
        n.capacity.as_ref().unwrap().leader_count as u64
    }

    fn should_balance(&self, rep: &PotentialReplacement) -> bool {
        self.count_policy.should_balance(rep)
    }

    fn balance_score(
        &self,
        node: &NodeCandidate,
        cands: &[NodeCandidate],
        from: bool,
    ) -> (f64 /* balance_score */, f64 /* converges_score */) {
        self.count_policy.balance_score(node, cands, from)
    }

    fn goal(&self) -> BalanceGoal {
        BalanceGoal::LeaderConvergence
    }
}
