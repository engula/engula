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

use self::{
    policy_leader_cnt::LeaderCountPolicy, policy_shard_cnt::ShardCountPolicy, source::NodeFilter,
};
use super::{metrics, OngoingStats, RootShared};
use crate::{bootstrap::REPLICA_PER_GROUP, Result};

#[cfg(test)]
mod sim_test;

mod policy_leader_cnt;
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
    pub enable_leader_balance: bool,
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
            enable_leader_balance: true,
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

pub struct GroupReplicaAction {
    pub group_id: u64,
    pub epoch: u64,
    pub source_node: u64,
    pub dest_node: u64,
}

#[derive(Default, Clone)]
pub struct NodeCandidate {
    node: NodeDesc,
    disk_full: bool,
    balance_score: f64,
    converges_score: f64,
    replica_cnt: f64,
}

impl NodeCandidate {
    fn worse(&self, o: &NodeCandidate) -> bool {
        score_compare_candidate(self, o) < 0.0
    }

    fn almost_same(&self, o: &NodeCandidate) -> bool {
        self.balance_score == o.balance_score && self.converges_score == o.converges_score
    }
}

fn score_compare_candidate(c: &NodeCandidate, o: &NodeCandidate) -> f64 {
    // the greater value, the more suitable for `c` than `o` to be a memeber of current group.
    if o.disk_full && !c.disk_full {
        return 3.0;
    }
    if c.disk_full && !o.disk_full {
        return -3.0;
    }
    if c.converges_score != o.converges_score {
        if c.converges_score > o.converges_score {
            return 2.0 + ((c.converges_score - o.converges_score) / 10.0);
        }
        return -(2.0 + ((o.converges_score - c.converges_score) / 10.0));
    }
    if c.balance_score != o.balance_score {
        if c.balance_score > o.balance_score {
            return 1.0 + ((c.balance_score - o.balance_score) / 10.0);
        }
        return -(1.0 + ((o.balance_score - c.balance_score) / 10.0));
    }
    let c_replica_count = c.replica_cnt;
    let o_replica_count = o.replica_cnt;

    if c_replica_count == o_replica_count {
        return 0.0;
    }
    if c_replica_count < o_replica_count {
        return (o_replica_count - c_replica_count) as f64 / (o_replica_count as f64);
    }
    -((c_replica_count - o_replica_count) as f64 / c_replica_count as f64)
}

fn node_replica_count(ongoing_stats: Arc<OngoingStats>, n: &NodeDesc) -> u64 {
    let mut cnt = n.capacity.as_ref().unwrap().replica_count as i64;
    let delta = ongoing_stats.get_node_delta(n.id);
    cnt += delta.replica_count;
    if cnt < 0 {
        cnt = 0;
    }
    cnt as u64
}

impl Ord for NodeCandidate {
    fn cmp(&self, other: &Self) -> Ordering {
        let score_cmp = score_compare_candidate(self, other);
        if score_cmp < 0.0 {
            return Ordering::Less;
        }
        if score_cmp > 0.0 {
            return Ordering::Greater;
        }
        Ordering::Equal
    }
}

impl PartialOrd for NodeCandidate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for NodeCandidate {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for NodeCandidate {}

pub struct PotentialReplacement {
    existing: NodeCandidate,
    candidates: Vec<NodeCandidate>,
}

struct BalanceOption {
    existing: NodeCandidate,
    candidates: Vec<NodeCandidate>,
}

pub trait BalancePolicy {
    fn should_balance(&self, rep: &PotentialReplacement) -> bool;

    fn balance_score(
        &self,
        node: &NodeCandidate,
        cands: &[NodeCandidate],
        from: bool,
    ) -> (f64 /* balance_score */, f64 /* converges_score */);
}

#[derive(Clone, Copy, Default)]
pub struct ByReplicaCountPolicy {}

impl BalancePolicy for ByReplicaCountPolicy {
    fn should_balance(&self, rep: &PotentialReplacement) -> bool {
        if rep.candidates.is_empty() {
            return false;
        }
        let (mean, min, max) = self.replica_count_threshold(&rep.candidates);
        let cnt = rep.existing.replica_cnt;
        if cnt > max {
            // balance if over max a lot.
            return true;
        }
        if cnt > mean {
            // balance if over mean and others is underfull.
            for c in &rep.candidates {
                let cand_cnt = c.replica_cnt;
                if cand_cnt < min {
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
        let current = node.replica_cnt;
        let (mean, min, max) = self.replica_count_threshold(cands);
        let mut balance_score = 0.0;
        if current < min {
            balance_score = 1.0;
        }
        if current > max {
            balance_score = -1.0;
        }
        let new_val = if from { current - 1.0 } else { current + 1.0 };
        let converges_score = if f64::abs(new_val - mean) >= f64::abs(current - mean) {
            1.0
        } else {
            0.0
        };
        (balance_score, converges_score)
    }
}

impl ByReplicaCountPolicy {
    fn replica_count_threshold(&self, cands: &[NodeCandidate]) -> (f64, f64, f64) {
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
            .fold(0f64, |t, c| t + c.replica_cnt) as f64;
        let cnt = cands.iter().filter(|c| c.node.capacity.is_some()).count() as f64;
        total / cnt
    }
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

    pub async fn compute_balance_action(
        &self,
        policy: &(dyn BalancePolicy + Send + Sync + 'static),
    ) -> Result<Vec<GroupReplicaAction>> {
        if !self.config.enable_replica_balance {
            return Ok(vec![]);
        }

        self.alloc_source.refresh_all().await?;

        let mut gaction = Vec::new();
        let groups = self.alloc_source.groups();
        for (group_id, desc) in &groups {
            let in_joint_or_learner = desc.replicas.iter().any(|r| {
                matches!(
                    ReplicaRole::from_i32(r.role).unwrap(),
                    ReplicaRole::IncomingVoter | ReplicaRole::DemotingVoter | ReplicaRole::Learner
                )
            });
            if in_joint_or_learner {
                // in-progress group should consider addition group action
                // after it leave middle status.
                continue;
            }
            let mut balance_opts = self.try_balance(policy, desc, true).await?;
            if balance_opts.is_none() {
                continue;
            }
            let (source_node, dest_node) = balance_opts.take().unwrap();
            gaction.push(GroupReplicaAction {
                group_id: group_id.to_owned(),
                epoch: desc.epoch,
                source_node,
                dest_node,
            });
            break;
        }
        Ok(gaction)
    }

    fn best_balance_target(
        &self,
        opts: &mut [BalanceOption],
    ) -> (Option<NodeCandidate>, Option<NodeCandidate>) {
        let mut best_idx = None;
        let mut best_target = None;
        let mut best_existing = None;
        for (i, opt) in opts.iter().enumerate() {
            if opt.candidates.is_empty() {
                continue;
            }
            let target = select_good(&opt.candidates);
            if target.is_none() {
                continue;
            }
            let target = target.unwrap();
            let existing = opt.existing.to_owned();
            let better_target = self.better_target(
                &target,
                &existing,
                best_target.to_owned(),
                best_existing.to_owned(),
            );
            if better_target.node.id == target.node.id {
                best_target = Some(target.to_owned());
                best_existing = Some(existing.to_owned());
                best_idx = Some(i);
            }
        }
        if best_idx.is_none() {
            return (None, None);
        }
        let target = best_target.as_ref().unwrap().to_owned();
        let best_opt = opts.get_mut(best_idx.unwrap()).unwrap();
        best_opt.candidates.retain(|c| c.node.id != target.node.id);
        (Some(target), Some(best_opt.existing.to_owned()))
    }

    fn better_target(
        &self,
        new_target: &NodeCandidate,
        new_existing: &NodeCandidate,
        old_target: Option<NodeCandidate>,
        old_existing: Option<NodeCandidate>,
    ) -> NodeCandidate {
        if old_target.is_none() {
            return new_target.to_owned();
        }
        let cmp_score1 = score_compare_candidate(new_target, new_existing);
        let cmp_score2 =
            score_compare_candidate(old_target.as_ref().unwrap(), old_existing.as_ref().unwrap());
        if almost_same_score(cmp_score1, cmp_score2) {
            if cmp_score1 > cmp_score2 {
                return new_target.to_owned();
            }
            if cmp_score1 < cmp_score2 {
                return old_target.as_ref().unwrap().to_owned();
            }
        }
        if new_target.worse(old_target.as_ref().unwrap()) {
            return old_target.as_ref().unwrap().to_owned();
        }
        new_target.to_owned()
    }

    async fn try_balance(
        &self,
        policy: &(dyn BalancePolicy + Send + Sync + 'static),
        group: &GroupDesc,
        within_voter: bool,
    ) -> Result<Option<(u64, u64)>> {
        let nodes = self.alloc_source.nodes(NodeFilter::Schedulable);

        // Prepare related replicas, it make them into three categories:
        // 1. `replica_to_balance`: the replicas that participate in balance
        // 2. `other_replica`: the replicas that not participate in balance but existed in the group
        // 3. `exclude_replica`: the replicas that forbid to allocate current type repli
        let mut other_replicas = Vec::new();
        let mut excluded_replicas = Vec::new();
        let voters = group
            .replicas
            .iter()
            .filter(|r| {
                if !nodes.iter().any(|n| n.id == r.node_id) {
                    return false;
                }
                if !matches!(ReplicaRole::from_i32(r.role).unwrap(), ReplicaRole::Voter) {
                    return false;
                }
                true
            })
            .collect::<Vec<_>>();
        let non_voters = group
            .replicas
            .iter()
            .filter(|r| {
                if !nodes.iter().any(|n| n.id == r.node_id) {
                    return false;
                }
                if matches!(ReplicaRole::from_i32(r.role).unwrap(), ReplicaRole::Voter) {
                    return false;
                }
                true
            })
            .collect::<Vec<_>>();

        let replica_to_balance = if within_voter {
            other_replicas.extend_from_slice(&non_voters);
            voters.to_owned()
        } else {
            other_replicas.extend_from_slice(&voters);
            excluded_replicas.extend_from_slice(&voters);
            non_voters.to_owned()
        };

        // find moveable nodes.
        // 1. the node contain particpated balance replica.
        // 2. full node need transfer-out in higher priority.
        let mut moveable_candidates = HashMap::new();
        let mut require_transfer_from = false;
        for repl_node in nodes
            .iter()
            .filter(|n| replica_to_balance.iter().any(|r| r.node_id == n.id))
        {
            let disk_full = check_node_full(repl_node);
            if disk_full {
                require_transfer_from = true;
            }
            let replica_cnt = node_replica_count(self.ongoing_stats.to_owned(), repl_node) as f64;
            moveable_candidates.insert(
                repl_node.id,
                NodeCandidate {
                    node: repl_node.to_owned(),
                    disk_full,
                    replica_cnt,
                    ..Default::default()
                },
            );
        }

        // find potential replacements, for each moveable replica to find replacable replicas:
        // 1. the replace candidate couldn't be allocate in the node already exist replica
        // 2. the replace candidate couldn't be allocate in forbid allocate nodes
        // 3. the replace candidate should not be disk full
        let mut potential_replacements = Vec::new();
        let mut require_transfer_to = false;
        for move_candidate in moveable_candidates.values() {
            let mut replace_candidates = Vec::new();
            for n in nodes
                .iter()
                .filter(|n| move_candidate.node.id != n.id)
                .filter(|n| !excluded_replicas.iter().any(|r| r.node_id == n.id))
            {
                let disk_full = check_node_full(n);
                let replica_cnt = node_replica_count(self.ongoing_stats.to_owned(), n) as f64;
                let cand = NodeCandidate {
                    node: n.to_owned(),
                    disk_full,
                    replica_cnt,
                    ..Default::default()
                };
                if !cand.worse(move_candidate) {
                    if !require_transfer_from && !require_transfer_to && move_candidate.worse(&cand)
                    {
                        require_transfer_to = true;
                    }
                    // replace replica better or almost same to moveable replica.
                    replace_candidates.push(cand);
                }
            }
            if !replace_candidates.is_empty() {
                replace_candidates.sort_by_key(|w| std::cmp::Reverse(w.to_owned()));
                let best_candidates = best(&replace_candidates);
                if best_candidates.is_empty() {
                    continue;
                }
                potential_replacements.push(PotentialReplacement {
                    existing: move_candidate.to_owned(),
                    candidates: best_candidates,
                });
            }
        }

        let mut need_balance = require_transfer_from || require_transfer_to;
        if !need_balance {
            for rep in &potential_replacements {
                if policy.should_balance(rep) {
                    need_balance = true;
                    break;
                }
            }
        }

        if !need_balance {
            return Ok(None);
        }

        // build the balance option with balance score by policy.
        let mut balance_opts = Vec::with_capacity(potential_replacements.len());
        for potential_replacement in potential_replacements.iter_mut() {
            let mut ex_cand = moveable_candidates
                .get(&potential_replacement.existing.node.id)
                .unwrap()
                .to_owned();
            ex_cand.replica_cnt =
                node_replica_count(self.ongoing_stats.to_owned(), &ex_cand.node) as f64;
            (ex_cand.balance_score, ex_cand.converges_score) =
                policy.balance_score(&ex_cand, &potential_replacement.candidates, true);

            let mut candidates = Vec::new();
            for candidate_node in &potential_replacement.candidates {
                if moveable_candidates.contains_key(&candidate_node.node.id) {
                    continue;
                }
                let mut c = candidate_node.to_owned();
                (c.balance_score, c.converges_score) =
                    policy.balance_score(&c, &potential_replacement.candidates, false);
                candidates.push(c);
            }

            if candidates.is_empty() {
                continue;
            }

            candidates.sort_by_key(|w| std::cmp::Reverse(w.to_owned()));

            let candidates = candidates
                .iter()
                .take_while(|c| !c.worse(&ex_cand))
                .cloned()
                .collect::<Vec<_>>();
            if candidates.is_empty() {
                continue;
            }

            balance_opts.push(BalanceOption {
                existing: ex_cand,
                candidates,
            });
        }

        if balance_opts.is_empty() {
            return Ok(None);
        }

        // check the result of simulate remove after running options, and skip option if new-added
        // replica be removed in next turn.
        loop {
            let (target, _existing) = self.best_balance_target(&mut balance_opts);
            if target.is_none() {
                break;
            }
            let mut exist_replica_candidates = replica_to_balance.to_owned();
            let fake_new_replica = ReplicaDesc {
                id: exist_replica_candidates.iter().map(|r| r.id).max().unwrap() + 1,
                node_id: target.as_ref().unwrap().node.id,
                ..Default::default()
            };
            exist_replica_candidates.push(&fake_new_replica);

            let replica_candidates = exist_replica_candidates.to_owned();

            // TODO: filter out out-of-date replicas from replica_candidates.

            let remove_candidate = self.sim_remove_target(
                policy,
                fake_new_replica.node_id,
                replica_candidates,
                exist_replica_candidates,
                other_replicas.to_owned(),
                within_voter,
            )?;

            if remove_candidate.is_none() {
                return Ok(None);
            }

            if remove_candidate.as_ref().unwrap().node.id != target.as_ref().unwrap().node.id {
                return Ok(Some((
                    remove_candidate.as_ref().unwrap().node.id,
                    target.as_ref().unwrap().node.id,
                )));
            }
        }

        Ok(None)
    }

    fn sim_remove_target(
        &self,
        policy: &(dyn BalancePolicy + Send + Sync + 'static),
        _remove_node: u64,
        replica_cands: Vec<&ReplicaDesc>,
        exist_cands: Vec<&ReplicaDesc>,
        other_replicas: Vec<&ReplicaDesc>,
        within_voter: bool,
    ) -> Result<Option<NodeCandidate>> {
        let nodes = self.alloc_source.nodes(NodeFilter::Schedulable);
        let candidate_nodes = replica_cands
            .iter()
            .map(|r| nodes.iter().find(|n| n.id == r.node_id).unwrap().to_owned())
            .collect::<Vec<_>>();
        if within_voter {
            self.remove_target(
                policy,
                candidate_nodes,
                exist_cands,
                other_replicas,
                within_voter,
            )
        } else {
            unimplemented!()
        }
    }

    fn remove_target(
        &self,
        policy: &(dyn BalancePolicy + Send + Sync + 'static),
        candidate_nodes: Vec<NodeDesc>,
        exist_cands: Vec<&ReplicaDesc>,
        other_replicas: Vec<&ReplicaDesc>,
        _within_voter: bool,
    ) -> Result<Option<NodeCandidate>> {
        assert!(!candidate_nodes.is_empty());
        let mut candidates = Vec::new();
        for n in &candidate_nodes {
            let replica_cnt = node_replica_count(self.ongoing_stats.to_owned(), n) as f64;
            candidates.push(NodeCandidate {
                node: n.to_owned(),
                disk_full: check_node_full(n),
                replica_cnt,
                ..Default::default()
            })
        }
        candidates.sort_by_key(|w| std::cmp::Reverse(w.to_owned()));

        let mut score_candidates = Vec::new();
        for c in &candidates {
            let mut c = c.to_owned();
            (c.balance_score, c.converges_score) = policy.balance_score(&c, &candidates, true);
            score_candidates.push(c);
        }
        score_candidates.sort_by_key(|w| std::cmp::Reverse(w.to_owned()));

        let worst_candidates = worst(&score_candidates);

        let exist_replicas = {
            let mut replicas = exist_cands.to_owned();
            replicas.extend_from_slice(&other_replicas);
            replicas
        };

        let bad_candidate = select_bad(&worst_candidates);
        if let Some(bad_candidate) = bad_candidate {
            for replica in exist_replicas {
                if replica.node_id == bad_candidate.node.id {
                    return Ok(Some(bad_candidate));
                }
            }
        }

        Ok(None)
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

        let candidate_nodes = candidate_nodes
            .into_iter()
            .map(|node| NodeCandidate {
                node: node.to_owned(),
                disk_full: check_node_full(&node),
                replica_cnt: node_replica_count(self.ongoing_stats.to_owned(), &node) as f64,
                ..Default::default()
            })
            .collect::<Vec<_>>();

        let policy = ByReplicaCountPolicy::default();
        let mut candidate_nodes = candidate_nodes
            .iter()
            .map(|n| {
                let (balance_score, converges_score) =
                    policy.balance_score(n, &candidate_nodes, false);
                NodeCandidate {
                    node: n.node.to_owned(),
                    disk_full: n.disk_full,
                    replica_cnt: n.replica_cnt,
                    balance_score,
                    converges_score,
                }
            })
            .collect::<Vec<_>>();

        candidate_nodes.sort();

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

    pub async fn compute_leader_action(&self) -> Result<Vec<LeaderAction>> {
        if !self.config.enable_leader_balance {
            return Ok(vec![]);
        }
        self.alloc_source.refresh_all().await?;
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
    let bests = best(cands);
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
    let candidates = worst(candidates);
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
