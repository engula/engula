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

use std::collections::HashSet;

use engula_client::{GroupClient, RouterGroupState};
use tokio::time::Instant;
use tracing::{info, warn};

use super::*;
use crate::root::{HeartbeatQueue, HeartbeatTask};

#[derive(Default, Clone, Debug)]
pub struct NodeCandidate {
    pub node: NodeDesc,
    pub disk_full: bool,
    pub balance_score: f64,
    pub converges_score: f64,
    pub balance_value: f64,
}

impl NodeCandidate {
    pub fn worse(&self, o: &NodeCandidate) -> bool {
        score_compare_candidate(self, o) < 0.0
    }

    pub fn almost_same(&self, o: &NodeCandidate) -> bool {
        self.balance_score == o.balance_score && self.converges_score == o.converges_score
    }
}

fn score_compare_candidate(c: &NodeCandidate, o: &NodeCandidate) -> f64 {
    // the greater value, the more suitable using `c` than `o`.
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
    let c_replica_count = c.balance_value;
    let o_replica_count = o.balance_value;

    if c_replica_count == o_replica_count {
        return 0.0;
    }
    if c_replica_count < o_replica_count {
        return (o_replica_count - c_replica_count) as f64 / (o_replica_count as f64);
    }
    -((c_replica_count - o_replica_count) as f64 / c_replica_count as f64)
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

#[derive(Debug)]
pub struct PotentialReplacement {
    pub existing: NodeCandidate,
    pub candidates: Vec<NodeCandidate>,
}

#[derive(Debug)]
struct BalanceOption {
    existing: NodeCandidate,
    candidates: Vec<NodeCandidate>,
}

#[derive(Default)]
pub struct BalanceTickContext {
    delta_value: HashMap<u64 /* node */, i64 /* delta value */>,
}

impl BalanceTickContext {
    pub fn get(&self, node: u64) -> i64 {
        *self.delta_value.get(&node).unwrap_or(&0)
    }

    pub fn update(&mut self, node: u64, v: i64) {
        let old = self.get(node);
        self.delta_value.insert(node, old + v);
    }
}

pub trait BalancePolicy {
    fn should_balance(&self, rep: &PotentialReplacement) -> bool;

    fn balance_score(
        &self,
        node: &NodeCandidate,
        cands: &[NodeCandidate],
        from: bool,
    ) -> (f64 /* balance_score */, f64 /* converges_score */);

    fn balance_value(&self, ctx: &BalanceTickContext, n: &NodeDesc) -> u64;

    fn delta_value(&self) -> i64;
}

#[derive(Clone)]
pub struct ReplicaBalancer<T: AllocSource> {
    shared: Arc<RootShared>,
    alloc_source: Arc<T>,
    config: RootConfig,
    heartbeat_queue: Arc<HeartbeatQueue>,
}

impl<T: AllocSource> ReplicaBalancer<T> {
    pub fn new(
        shared: Arc<RootShared>,
        alloc_source: Arc<T>,
        config: RootConfig,
        heartbeat_queue: Arc<HeartbeatQueue>,
    ) -> Self {
        Self {
            shared,
            alloc_source,
            config,
            heartbeat_queue,
        }
    }
}

impl<T: AllocSource> ReplicaBalancer<T> {
    pub async fn need_balance(
        &self,
        policy: &(dyn BalancePolicy + Send + Sync + 'static),
    ) -> Result<bool> {
        self.alloc_source.refresh_all().await?;
        Ok(self
            .compute_balance_action(&BalanceTickContext::default(), policy)
            .await?
            .is_some())
    }

    pub async fn balance_replica(
        &self,
        policy: &(dyn BalancePolicy + Send + Sync + 'static),
    ) -> Result<()> {
        self.alloc_source.refresh_all().await?;
        let mut related_nodes = HashSet::new();
        let mut tick_ctx = BalanceTickContext::default();
        while let Some(action) = self.compute_balance_action(&tick_ctx, policy).await? {
            info!(
                "try balance replica, group: {}, epoch: {}, src_node: {}, dest_node: {}",
                action.group.id, action.group.epoch, action.source_node, action.dest_node
            );

            self.reallocate_replica(&action).await?;

            related_nodes.insert(action.source_node);
            related_nodes.insert(action.dest_node);

            tick_ctx.update(action.source_node, -policy.delta_value());
            tick_ctx.update(action.dest_node, policy.delta_value());
        }
        if !related_nodes.is_empty() {
            info!("schedule heartbeat after repl balance");
            let now = Instant::now();
            for n in &related_nodes {
                self.heartbeat_queue
                    .try_schedule(
                        vec![HeartbeatTask {
                            node_id: n.to_owned(),
                        }],
                        now,
                    )
                    .await;
            }
            info!("schedule heartbeat after repl balance, done");
        }
        Ok(())
    }

    async fn compute_balance_action(
        &self,
        ctx: &BalanceTickContext,
        policy: &(dyn BalancePolicy + Send + Sync + 'static),
    ) -> Result<Option<ReplicaBalanceAction>> {
        if !self.config.enable_replica_balance {
            return Ok(None);
        }
        let groups = self.alloc_source.groups();
        for (group_id, desc) in &groups {
            if *group_id == ROOT_GROUP_ID {
                continue;
            }
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
            let balance_opts = self.try_balance_group(ctx, policy, desc, true).await?;
            if balance_opts.is_none() {
                continue;
            }
            let action = balance_opts.unwrap();
            return Ok(Some(action));
        }
        Ok(None)
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

    async fn try_balance_group(
        &self,
        ctx: &BalanceTickContext,
        policy: &(dyn BalancePolicy + Send + Sync + 'static),
        group: &GroupDesc,
        within_voter: bool,
    ) -> Result<Option<ReplicaBalanceAction>> {
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
            let balance_value = policy.balance_value(ctx, repl_node) as f64;
            moveable_candidates.insert(
                repl_node.id,
                NodeCandidate {
                    node: repl_node.to_owned(),
                    disk_full,
                    balance_value,
                    ..Default::default()
                },
            );
        }

        // find potential replacements, for each moveable replica to find replacable replicas:
        // 1. the replace candidate couldn't be allocate in the node already exist replica
        // 2. the replace candidate couldn't be allocate in forbid allocate nodes
        // 3. the replace candidate should not be disk full
        let mut potential_replacements = Vec::new();
        for move_candidate in moveable_candidates.values() {
            let mut replace_candidates = Vec::new();
            for n in nodes
                .iter()
                .filter(|n| move_candidate.node.id != n.id)
                .filter(|n| !excluded_replicas.iter().any(|r| r.node_id == n.id))
            {
                let disk_full = check_node_full(n);
                let balance_value = policy.balance_value(ctx, n) as f64;
                let cand = NodeCandidate {
                    node: n.to_owned(),
                    disk_full,
                    balance_value,
                    ..Default::default()
                };
                if !cand.worse(move_candidate) {
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

        let mut need_balance = require_transfer_from;
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
            ex_cand.balance_value = policy.balance_value(ctx, &ex_cand.node) as f64;
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

        // check the result of simulate remove after running options, and skip option if
        // new-added replica be removed in next turn.
        loop {
            let (target, existing) = self.best_balance_target(&mut balance_opts);
            info!(
                "balance replica move: {:?} target: {:?}, from: {:?}",
                balance_opts, target, existing,
            );
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

            let nodes = self.alloc_source.nodes(NodeFilter::Schedulable);
            let replica_candidates = exist_replica_candidates
                .iter()
                .map(|r| {
                    let n = nodes.iter().find(|n| n.id == r.node_id).unwrap();
                    let mut n = NodeCandidate {
                        node: n.to_owned(),
                        balance_value: (policy.balance_value(ctx, n)) as f64,
                        ..Default::default()
                    };
                    if n.node.id == fake_new_replica.node_id {
                        n.balance_value += 1.0 // TODO: add qps instead of 1 when using qps
                    }
                    n
                })
                .collect::<Vec<_>>();

            // TODO: filter out out-of-date replicas from replica_candidates.

            let remove_candidate = self.sim_remove_target(
                policy,
                replica_candidates,
                exist_replica_candidates,
                other_replicas.to_owned(),
                within_voter,
            )?;

            if remove_candidate.is_none() {
                return Ok(None);
            }

            if remove_candidate.as_ref().unwrap().node.id == target.as_ref().unwrap().node.id {
                continue;
            }
            let source_node = remove_candidate.as_ref().unwrap().node.id;
            let source_repl = group
                .replicas
                .iter()
                .find(|r| r.node_id == source_node)
                .unwrap();
            let (shed_leader, source_replica_id, source_term) =
                if let Some(source_repl_state) = self.alloc_source.replica_state(&source_repl.id) {
                    (
                        matches!(
                            RaftRole::from_i32(source_repl_state.role).unwrap(),
                            RaftRole::Leader
                        ),
                        source_repl_state.replica_id,
                        source_repl_state.term,
                    )
                } else {
                    (false, 0, 0)
                };
            return Ok(Some(ReplicaBalanceAction {
                group: group.to_owned(),
                source_node,
                dest_node: target.as_ref().unwrap().node.id,
                shed_leader,
                source_replica_id,
                source_term,
            }));
        }
        Ok(None)
    }

    fn sim_remove_target(
        &self,
        policy: &(dyn BalancePolicy + Send + Sync + 'static),
        node_candidates: Vec<NodeCandidate>,
        exist_cands: Vec<&ReplicaDesc>,
        other_replicas: Vec<&ReplicaDesc>,
        within_voter: bool,
    ) -> Result<Option<NodeCandidate>> {
        if within_voter {
            self.remove_target(
                policy,
                node_candidates,
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
        candidate_nodes: Vec<NodeCandidate>,
        exist_cands: Vec<&ReplicaDesc>,
        other_replicas: Vec<&ReplicaDesc>,
        _within_voter: bool,
    ) -> Result<Option<NodeCandidate>> {
        assert!(!candidate_nodes.is_empty());
        let mut candidates = Vec::new();
        for n in &candidate_nodes {
            candidates.push(NodeCandidate {
                node: n.node.to_owned(),
                disk_full: check_node_full(&n.node),
                balance_value: n.balance_value,
                ..Default::default()
            })
        }

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

    async fn reallocate_replica(&self, action: &ReplicaBalanceAction) -> Result<()> {
        let schema = self.shared.schema()?;
        let group_desc = action.group.to_owned();

        if action.shed_leader {
            let transferee = group_desc
                .replicas
                .iter()
                .find(|r| r.node_id != action.source_node)
                .unwrap();
            info!("need transfer leader before reallocate replica, group: {}, src_node: {}, dest_node: {}", group_desc.id, action.source_node, transferee.node_id);
            let r = self
                .try_transfer_leader(
                    group_desc.to_owned(),
                    (action.source_replica_id, action.source_term),
                    transferee.id,
                )
                .await;
            match r {
                Ok(_) => {}
                Err(err) => {
                    warn!(group = group_desc.id, replica = transferee.id, err = ?err, "shed leader in source replica fail, retry in next tick");
                    metrics::RECONCILE_RETRY_TASK_TOTAL.reallocate_replica.inc();
                    return Err(err);
                }
            };
        }

        let src_replica = group_desc
            .replicas
            .iter()
            .find(|r| r.id == action.source_replica_id)
            .unwrap();

        let next_replica = schema.next_replica_id().await?;

        match self
            .try_move_replica(
                group_desc.to_owned(),
                (action.source_replica_id, action.source_term),
                ReplicaDesc {
                    id: next_replica,
                    node_id: action.dest_node,
                    role: ReplicaRole::Voter as i32,
                },
                src_replica.to_owned(),
            )
            .await
        {
            Ok(_) => Ok(()),
            Err(err) => {
                warn!(
                    group = group_desc.id,
                    src_node = action.source_node,
                    dest_node = action.dest_node,
                    err = ?err,
                    "move replica meet error and retry later"
                );
                metrics::RECONCILE_RETRY_TASK_TOTAL.reallocate_replica.inc();
                Err(err)
            }
        }
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

    async fn try_move_replica(
        &self,
        group: GroupDesc,
        leader_state: (u64 /* id */, u64 /* term */),
        incoming_replica: ReplicaDesc,
        outgoing_replica: ReplicaDesc,
    ) -> Result<ScheduleState> {
        let (src_node, dest_node) = (outgoing_replica.node_id, incoming_replica.node_id);
        info!(
            group = group.id,
            src_node = src_node,
            dest_node = dest_node,
            "start move replica"
        );
        let mut group_client = GroupClient::new(
            RouterGroupState {
                id: group.id,
                epoch: group.epoch,
                leader_state: Some(leader_state),
                replicas: group
                    .replicas
                    .iter()
                    .map(|g| (g.id, g.to_owned()))
                    .collect::<HashMap<_, _>>(),
            },
            self.shared.provider.router.clone(),
            self.shared.provider.conn_manager.clone(),
        );
        let current_state = group_client
            .move_replicas(vec![incoming_replica], vec![outgoing_replica])
            .await?;

        info!(
            group = group.id,
            src_node = src_node,
            dest_node = dest_node,
            "move replica submitted"
        );
        Ok(current_state)
    }
}
