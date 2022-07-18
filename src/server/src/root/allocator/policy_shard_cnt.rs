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

use engula_api::server::v1::{GroupDesc, ShardDesc};
use tracing::trace;

use super::{AllocSource, ReallocateShard, ShardAction};
use crate::{bootstrap::ROOT_GROUP_ID, root::allocator::BalanceStatus, Result};

pub struct ShardCountPolicy<T: AllocSource> {
    alloc_source: Arc<T>,
}

impl<T: AllocSource> ShardCountPolicy<T> {
    pub fn with(alloc_source: Arc<T>) -> Self {
        Self { alloc_source }
    }

    pub fn allocate_shard(&self, n: usize) -> Result<Vec<GroupDesc>> {
        let mut groups = self.current_user_groups();
        if groups.is_empty() {
            return Ok(vec![]);
        }
        groups.sort_by(|g1, g2| g1.shards.len().cmp(&g2.shards.len()));
        Ok(groups.into_iter().take(n).collect())
    }

    pub fn compute_balance(&self) -> Result<Vec<ShardAction>> {
        let mean_cnt = self.mean_shard_count();
        let candicate_groups = self.current_user_groups();

        let ranked_candicates = Self::rank_group_for_balance(candicate_groups, mean_cnt);
        trace!(
            scored_nodes = ?ranked_candicates.iter().map(|(g, s)| format!("{}-{}({:?})", g.id, g.shards.len(), s)).collect::<Vec<_>>(),
            mean = mean_cnt,
            "group ranked by shard count",
        );
        for (src_group, status) in &ranked_candicates {
            if *status != BalanceStatus::Overfull {
                break;
            }
            if let Some(action) = self.rebalance_target(src_group, &ranked_candicates) {
                return Ok(vec![action]);
            }
        }

        Ok(vec![])
    }

    fn mean_shard_count(&self) -> f64 {
        let groups = self.current_user_groups();
        let total_shards = groups.iter().map(|n| n.shards.len() as u64).sum::<u64>() as f64;
        total_shards / (groups.len() as f64)
    }

    fn rank_group_for_balance(
        gs: Vec<GroupDesc>,
        mean_cnt: f64,
    ) -> Vec<(GroupDesc, BalanceStatus)> {
        let mut with_status = gs
            .into_iter()
            .map(|n| {
                let shard_num = n.shards.len() as f64;
                let s = Self::group_balance_state(shard_num, mean_cnt);
                (n, s)
            })
            .collect::<Vec<(GroupDesc, BalanceStatus)>>();
        with_status.sort_by(|n1, n2| {
            if (n2.1 == BalanceStatus::Overfull) && (n1.1 != BalanceStatus::Overfull) {
                return Ordering::Greater;
            }
            if (n2.1 == BalanceStatus::Underfull) && (n1.1 != BalanceStatus::Underfull) {
                return Ordering::Less;
            }
            n2.0.shards.len().cmp(&n1.0.shards.len())
        });
        with_status
    }

    fn group_balance_state(shard_num: f64, mean: f64) -> BalanceStatus {
        const THRESHOLD_FRACTION: f64 = 0.05;
        const MIN_RANGE_DELTA: f64 = 2.0;
        let delta = f64::min(mean as f64 * THRESHOLD_FRACTION, MIN_RANGE_DELTA);
        if shard_num > mean + delta {
            return BalanceStatus::Overfull;
        }
        if shard_num < mean - delta {
            return BalanceStatus::Underfull;
        }
        BalanceStatus::Balanced
    }

    fn rebalance_target(
        &self,
        source_group: &GroupDesc,
        ranked_candicates: &[(GroupDesc, BalanceStatus)],
    ) -> Option<ShardAction> {
        let mean = self.mean_shard_count();
        for (target, state) in ranked_candicates.iter().rev() {
            if *state != BalanceStatus::Underfull {
                break;
            }
            let sim_count = (target.shards.len() + 1) as f64;
            if Self::group_balance_state(sim_count, mean) == BalanceStatus::Overfull {
                continue;
            }
            let source_shard = self.preferred_remove_shard(source_group, target)?;
            return Some(ShardAction::Migrate(ReallocateShard {
                shard: source_shard.id,
                source_group: source_group.id,
                target_group: target.id,
            }));
        }
        None
    }

    fn preferred_remove_shard(
        &self,
        src_group: &GroupDesc,
        _target_group: &GroupDesc,
    ) -> Option<ShardDesc> {
        let replicas = src_group.shards.to_owned();
        // TODO: ranking shards and choose the preferred one
        replicas.get(0).map(ToOwned::to_owned)
    }

    fn current_user_groups(&self) -> Vec<GroupDesc> {
        let groups = self.alloc_source.groups();
        groups
            .values()
            .filter(|g| g.id != ROOT_GROUP_ID)
            .map(ToOwned::to_owned)
            .collect()
    }
}
