// // Copyright 2022 The Engula Authors.
// //
// // Licensed under the Apache License, Version 2.0 (the "License");
// // you may not use this file except in compliance with the License.
// // You may obtain a copy of the License at
// //
// // http://www.apache.org/licenses/LICENSE-2.0
// //
// // Unless required by applicable law or agreed to in writing, software
// // distributed under the License is distributed on an "AS IS" BASIS,
// // WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// // See the License for the specific language governing permissions and
// // limitations under the License.

// use std::{
//     collections::{hash_map::Entry, HashMap},
//     sync::{
//         atomic::{AtomicU64, Ordering},
//         Mutex,
//     },
// };

// use engula_api::server::v1::*;

// use super::*;
// use crate::{
//     bootstrap::REPLICA_PER_GROUP, root::allocator::source::NodeFilter, runtime::ExecutorOwner,
// };

// #[test]
// fn sim_boostrap_join_node_balance() {
//     let executor_owner = ExecutorOwner::new(1);
//     let executor = executor_owner.executor();
//     executor.block_on(async {
//         let p = Arc::new(MockInfoProvider::new());
//         let d = Arc::new(OngoingStats::default());
//         let a = Allocator::new(p.clone(), d.clone(), RootConfig::default());

//         let policy = ReplicaCountPolicy::default();

//         println!("1. boostrap and no need rebalance");
//         p.set_groups(vec![GroupDesc {
//             id: 1,
//             epoch: 0,
//             shards: vec![],
//             replicas: vec![ReplicaDesc {
//                 id: 1,
//                 node_id: 1,
//                 role: ReplicaRole::Voter.into(),
//             }],
//         }]);
//         p.set_nodes(vec![NodeDesc {
//             id: 1,
//             addr: "".into(),
//             capacity: Some(NodeCapacity {
//                 cpu_nums: 2.0,
//                 replica_count: 1,
//                 leader_count: 1,
//             }),
//             status: NodeStatus::Active as i32,
//         }]);
//         p.set_replica_states(vec![ReplicaState {
//             replica_id: 1,
//             group_id: 1,
//             term: 0,
//             voted_for: 0,
//             role: RaftRole::Leader.into(),
//             node_id: 1,
//         }]);

//         let act = a.compute_group_action().await.unwrap();
//         assert!(matches!(act, GroupAction::Noop));
//         p.display();

//         println!("2. two node joined");
//         let mut nodes = p.nodes(NodeFilter::All);
//         nodes.extend_from_slice(&[
//             NodeDesc {
//                 id: 2,
//                 addr: "".into(),
//                 capacity: Some(NodeCapacity {
//                     cpu_nums: 2.0,
//                     replica_count: 0,
//                     leader_count: 0,
//                 }),
//                 status: NodeStatus::Active as i32,
//             },
//             NodeDesc {
//                 id: 3,
//                 addr: "".into(),
//                 capacity: Some(NodeCapacity {
//                     cpu_nums: 2.0,
//                     replica_count: 0,
//                     leader_count: 0,
//                 }),
//                 status: NodeStatus::Active as i32,
//             },
//         ]);
//         p.set_nodes(nodes);
//         p.display();

//         println!("3. group0 be repaired");
//         p.set_groups(vec![GroupDesc {
//             id: 1,
//             epoch: 1,
//             shards: vec![],
//             replicas: vec![
//                 ReplicaDesc {
//                     id: 1,
//                     node_id: 1,
//                     role: ReplicaRole::Voter.into(),
//                 },
//                 ReplicaDesc {
//                     id: 2,
//                     node_id: 2,
//                     role: ReplicaRole::Voter.into(),
//                 },
//                 ReplicaDesc {
//                     id: 3,
//                     node_id: 3,
//                     role: ReplicaRole::Voter.into(),
//                 },
//             ],
//         }]);
//         p.set_replica_states(vec![
//             ReplicaState {
//                 replica_id: 1,
//                 group_id: 1,
//                 term: 1,
//                 voted_for: 0,
//                 role: RaftRole::Leader.into(),
//                 node_id: 1,
//             },
//             ReplicaState {
//                 replica_id: 2,
//                 group_id: 1,
//                 term: 1,
//                 voted_for: 0,
//                 role: RaftRole::Follower.into(),
//                 node_id: 2,
//             },
//             ReplicaState {
//                 replica_id: 3,
//                 group_id: 1,
//                 term: 1,
//                 voted_for: 0,
//                 role: RaftRole::Follower.into(),
//                 node_id: 3,
//             },
//         ]);
//         p.display();

//         let mut group_id_gen = 2;
//         let mut replica_id_gen = 4;

//         let act = a.compute_group_action().await.unwrap();
//         match act {
//             GroupAction::Add(n) => {
//                 for _ in 0..n {
//                     let nodes = a
//                         .allocate_group_replica(vec![], REPLICA_PER_GROUP)
//                         .await
//                         .unwrap();
//                     println!(
//                         "alloc group {} in {:?}",
//                         group_id_gen,
//                         nodes.iter().map(|n| n.id).collect::<Vec<u64>>()
//                     );
//                     let mut groups = p.groups();
//                     let mut replica_states = p.replica_states();
//                     let mut replicas = Vec::new();
//                     let mut first = true;
//                     for n in nodes {
//                         replicas.push(ReplicaDesc {
//                             id: replica_id_gen,
//                             node_id: n.id,
//                             role: ReplicaRole::Voter.into(),
//                         });
//                         let role = if first {
//                             first = false;
//                             RaftRole::Leader.into()
//                         } else {
//                             RaftRole::Follower.into()
//                         };
//                         replica_states.push(ReplicaState {
//                             replica_id: replica_id_gen,
//                             group_id: group_id_gen,
//                             term: 0,
//                             voted_for: 0,
//                             role,
//                             node_id: n.id,
//                         });
//                         replica_id_gen += 1;
//                     }
//                     groups.insert(
//                         group_id_gen,
//                         GroupDesc {
//                             id: group_id_gen,
//                             epoch: 0,
//                             shards: vec![],
//                             replicas,
//                         },
//                     );
//                     p.set_groups(groups.values().into_iter().map(ToOwned::to_owned).collect());
//                     p.set_replica_states(replica_states);
//                     group_id_gen += 1;
//                 }
//             }
//             _ => unreachable!(),
//         }
//         println!("4. group alloc works and group & replicas balanced");
//         let act = a.compute_group_action().await.unwrap();
//         assert!(matches!(act, GroupAction::Noop));
//         let ract = a.compute_balance_action(&policy).await.unwrap();
//         assert!(ract.is_empty());
//         p.display();

//         println!("5. assign shard in groups");
//         let cg = a.place_group_for_shard(9).await.unwrap();
//         for id in 0..9 {
//             let group = cg.get(id % cg.len()).unwrap();
//             p.assign_shard(group.id);
//             println!(
//                 "assign shard to group {}, prev_shard_cnt: {}",
//                 group.id,
//                 group.shards.len()
//             );
//         }

//         println!("6. node 4 joined");
//         let mut nodes = p.nodes(NodeFilter::All);
//         nodes.extend_from_slice(&[NodeDesc {
//             id: 4,
//             addr: "".into(),
//             capacity: Some(NodeCapacity {
//                 cpu_nums: 2.0,
//                 replica_count: 0,
//                 leader_count: 0,
//             }),
//             status: NodeStatus::Active as i32,
//         }]);
//         p.set_nodes(nodes);
//         p.display();

//         println!("7. balance group for new node");
//         let act = a.compute_group_action().await.unwrap();
//         match act {
//             GroupAction::Add(n) => {
//                 assert!(matches!(n, 2));
//                 for _ in 0..n {
//                     let nodes = a
//                         .allocate_group_replica(vec![], REPLICA_PER_GROUP)
//                         .await
//                         .unwrap();
//                     println!(
//                         "alloc group {} in {:?}",
//                         group_id_gen,
//                         nodes.iter().map(|n| n.id).collect::<Vec<u64>>()
//                     );
//                     let mut groups = p.groups();
//                     let mut replica_states = p.replica_states();
//                     let mut replicas = Vec::new();
//                     let mut first = true;
//                     for n in nodes {
//                         replicas.push(ReplicaDesc {
//                             id: replica_id_gen,
//                             node_id: n.id,
//                             role: ReplicaRole::Voter.into(),
//                         });
//                         let role = if first {
//                             first = false;
//                             RaftRole::Leader.into()
//                         } else {
//                             RaftRole::Follower.into()
//                         };
//                         replica_states.push(ReplicaState {
//                             replica_id: replica_id_gen,
//                             group_id: group_id_gen,
//                             term: 0,
//                             voted_for: 0,
//                             role,
//                             node_id: n.id,
//                         });
//                         replica_id_gen += 1;
//                     }
//                     groups.insert(
//                         group_id_gen,
//                         GroupDesc {
//                             id: group_id_gen,
//                             epoch: 0,
//                             shards: vec![],
//                             replicas,
//                         },
//                     );
//                     p.set_groups(groups.values().into_iter().map(ToOwned::to_owned).collect());
//                     p.set_replica_states(replica_states);
//                     group_id_gen += 1;
//                 }
//             }
//             _ => unreachable!(),
//         }

//         // cluster group balanced.
//         let act = a.compute_group_action().await.unwrap();
//         assert!(matches!(act, GroupAction::Noop));
//         p.display();

//         println!("8. balance replica between nodes");
//         let racts = a.compute_balance_action(&policy).await.unwrap();
//         assert!(!racts.is_empty());
//         for act in &racts {
//             println!(
//                 "move group {} replica {} to {}",
//                 act.group_id, act.source_node, act.dest_node
//             );
//             p.move_replica(act.group_id, act.source_node, act.dest_node)
//         }
//         loop {
//             let racts = a.compute_balance_action(&policy).await.unwrap();
//             if racts.is_empty() {
//                 break;
//             }
//             for act in &racts {
//                 println!(
//                     "move group {} replica {} to {}",
//                     act.group_id, act.source_node, act.dest_node
//                 );
//                 p.move_replica(act.group_id, act.source_node, act.dest_node)
//             }
//         }
//         let racts = a.compute_balance_action(&policy).await.unwrap();
//         assert!(racts.is_empty());
//         p.display();

//         println!("9. balance shards between groups");
//         let sact = a.compute_shard_action().await.unwrap();
//         assert!(!sact.is_empty());
//         for act in &sact {
//             match act {
//                 ShardAction::Migrate(ReallocateShard {
//                     shard,
//                     source_group,
//                     target_group,
//                 }) => {
//                     println!(
//                         "move shard {} from {} to {}",
//                         shard, source_group, target_group
//                     );
//                     p.move_shards(
//                         source_group.to_owned(),
//                         target_group.to_owned(),
//                         shard.to_owned(),
//                     );
//                 }
//             }
//         }
//         let sact = a.compute_shard_action().await.unwrap();
//         assert!(!sact.is_empty());
//         for act in &sact {
//             match act {
//                 ShardAction::Migrate(ReallocateShard {
//                     shard,
//                     source_group,
//                     target_group,
//                 }) => {
//                     println!(
//                         "move shard {} from {} to {}",
//                         shard, source_group, target_group
//                     );
//                     p.move_shards(
//                         source_group.to_owned(),
//                         target_group.to_owned(),
//                         shard.to_owned(),
//                     );
//                 }
//             }
//         }
//         let sact = a.compute_shard_action().await.unwrap();
//         assert!(sact.is_empty());
//         p.display();

//         println!("10. try balance leader between nodes");
//         let policy = LeaderCountPolicy::default();
//         loop {
//             let lact = a.compute_balance_action(&policy).await.unwrap();
//             if lact.is_empty() {
//                 break;
//             }
//             let lact = lact
//                 .iter()
//                 .map(|act| {
//                     LeaderAction::Shed(TransferLeader {
//                         group: act.group_id,
//                         src_node: act.source_node,
//                         target_node: act.dest_node,
//                         epoch: act.epoch,
//                     })
//                 })
//                 .collect::<Vec<_>>();
//             for act in &lact {
//                 match act {
//                     LeaderAction::Shed(action) => {
//                         println!(
//                             "transfer group {} leader from {} to {}",
//                             action.group, action.src_node, action.target_node,
//                         );
//                         p.transfer_leader(action.group, action.src_node, action.target_node);
//                     }
//                 }
//             }
//         }
//         p.display();

//         println!("done");
//     });
// }

// pub struct MockInfoProvider {
//     nodes: Arc<Mutex<Vec<NodeDesc>>>,
//     groups: Arc<Mutex<GroupInfo>>,
//     replicas: Arc<Mutex<HashMap<u64, ReplicaState>>>,
//     shard_id_gen: AtomicU64,
// }

// #[derive(Default)]
// struct GroupInfo {
//     descs: HashMap<u64, GroupDesc>,
//     node_replicas: HashMap<u64, Vec<(ReplicaDesc, u64)>>,
// }

// impl MockInfoProvider {
//     pub fn new() -> Self {
//         Self {
//             nodes: Default::default(),
//             groups: Default::default(),
//             replicas: Default::default(),
//             shard_id_gen: AtomicU64::new(1),
//         }
//     }
// }

// #[crate::async_trait]
// impl AllocSource for MockInfoProvider {
//     async fn refresh_all(&self) -> Result<()> {
//         Ok(())
//     }

//     fn nodes(&self, _: NodeFilter) -> Vec<NodeDesc> {
//         let nodes = self.nodes.lock().unwrap();
//         nodes.to_owned()
//     }

//     fn groups(&self) -> HashMap<u64, GroupDesc> {
//         let groups = self.groups.lock().unwrap();
//         groups.descs.to_owned()
//     }

//     fn node_replicas(&self, node_id: &u64) -> Vec<(ReplicaDesc, u64)> {
//         let groups = self.groups.lock().unwrap();
//         groups
//             .node_replicas
//             .get(node_id)
//             .map(ToOwned::to_owned)
//             .unwrap_or_default()
//     }

//     fn replica_state(&self, replica_id: &u64) -> Option<ReplicaState> {
//         let replica_info = self.replicas.lock().unwrap();
//         replica_info.get(replica_id).map(ToOwned::to_owned)
//     }

//     fn replica_states(&self) -> Vec<ReplicaState> {
//         let replica_info = self.replicas.lock().unwrap();
//         replica_info.iter().map(|e| e.1.to_owned()).collect()
//     }
// }

// impl MockInfoProvider {
//     fn set_nodes(&self, ns: Vec<NodeDesc>) {
//         let mut nodes = self.nodes.lock().unwrap();
//         let _ = std::mem::replace(&mut *nodes, ns);
//     }

//     fn set_groups(&self, gs: Vec<GroupDesc>) {
//         let mut groups = self.groups.lock().unwrap();
//         let mut node_replicas: HashMap<u64, Vec<(ReplicaDesc, u64)>> = HashMap::new();
//         for group in gs.iter() {
//             for replica in &group.replicas {
//                 match node_replicas.entry(replica.node_id) {
//                     Entry::Occupied(mut ent) => {
//                         (*ent.get_mut()).push((replica.to_owned(), group.id.to_owned()));
//                     }
//                     Entry::Vacant(ent) => {
//                         ent.insert(vec![(replica.to_owned(), group.id.to_owned())]);
//                     }
//                 };
//             }
//         }

//         // test only fix node.replica logic
//         let mut nodes = self.nodes(NodeFilter::All);
//         for n in nodes.iter_mut() {
//             let mut cap = n.capacity.take().unwrap();
//             cap.replica_count = node_replicas
//                 .get(&n.id)
//                 .map(|c| c.len() as u64)
//                 .unwrap_or_default();
//             n.capacity = Some(cap)
//         }
//         self.set_nodes(nodes);

//         let descs = gs.into_iter().map(|g| (g.id, g)).collect();
//         let _ = std::mem::replace(
//             &mut *groups,
//             GroupInfo {
//                 descs,
//                 node_replicas,
//             },
//         );
//     }

//     fn set_replica_states(&self, rs: Vec<ReplicaState>) {
//         let mut replicas = self.replicas.lock().unwrap();

//         // test only, maintain leader count in node.
//         let mut nodes = self.nodes(NodeFilter::All);
//         let groups = self.groups();
//         let mut node_leader = HashMap::new();
//         for r in &rs {
//             if r.role != RaftRole::Leader as i32 {
//                 continue;
//             }
//             let group = groups.get(&r.group_id).unwrap();
//             let desc = group
//                 .replicas
//                 .iter()
//                 .find(|d| d.id == r.replica_id)
//                 .unwrap();
//             match node_leader.entry(&desc.node_id) {
//                 Entry::Occupied(mut ent) => {
//                     let v = ent.get_mut();
//                     *v += 1;
//                 }
//                 Entry::Vacant(ent) => {
//                     ent.insert(1);
//                 }
//             }
//         }
//         for n in nodes.iter_mut() {
//             let mut cap = n.capacity.take().unwrap();
//             cap.leader_count = node_leader
//                 .get(&n.id)
//                 .map(ToOwned::to_owned)
//                 .unwrap_or_default();
//             n.capacity = Some(cap);
//         }
//         self.set_nodes(nodes);

//         let id_to_state = rs
//             .into_iter()
//             .map(|r| (r.replica_id, r))
//             .collect::<HashMap<u64, ReplicaState>>();
//         let _ = std::mem::replace(&mut *replicas, id_to_state);
//     }

//     pub fn move_replica(&self, group: u64, src_node: u64, dest_node: u64) {
//         let mut groups = self.groups();
//         let group = groups.get_mut(&group).unwrap();
//         for replica in group.replicas.iter_mut() {
//             if replica.node_id == src_node {
//                 replica.node_id = dest_node;
//                 let mut states = self.replica_states();
//                 for state in states.iter_mut() {
//                     if state.replica_id == replica.id {
//                         state.node_id = dest_node;
//                         break;
//                     }
//                 }
//                 self.set_replica_states(states);
//                 break;
//             }
//         }
//         self.set_groups(groups.values().map(ToOwned::to_owned).collect());
//     }

//     pub fn transfer_leader(&self, group: u64, src_node: u64, dest_node: u64) {
//         let mut states = self.replica_states();
//         for state in states.iter_mut() {
//             if state.group_id != group {
//                 continue;
//             }
//             if state.node_id == src_node {
//                 state.role = RaftRole::Follower.into();
//             }
//             if state.node_id == dest_node {
//                 state.role = RaftRole::Leader.into();
//             }
//         }
//         self.set_replica_states(states);
//     }

//     pub fn move_shards(&self, sgroup: u64, tgroup: u64, shard: u64) {
//         let mut groups = self.groups();

//         let mut shard_desc = None;
//         for group in groups.values_mut() {
//             if group.id == sgroup {
//                 group.shards.retain(|s| {
//                     if s.id == shard {
//                         shard_desc = Some(s.to_owned())
//                     }
//                     s.id != shard
//                 });
//                 break;
//             }
//         }

//         if let Some(shard_desc) = shard_desc {
//             for group in groups.values_mut() {
//                 if group.id == tgroup {
//                     group.shards.push(shard_desc);
//                     break;
//                 }
//             }
//         }

//         self.set_groups(groups.values().map(ToOwned::to_owned).collect());
//     }

//     pub fn assign_shard(&self, group_id: u64) {
//         let mut groups = self.groups();
//         for group in groups.values_mut() {
//             if group.id == group_id {
//                 let s = ShardDesc {
//                     id: self.shard_id_gen.fetch_add(1, Ordering::Relaxed),
//                     ..Default::default()
//                 };
//                 group.shards.push(s);
//             }
//         }
//         self.set_groups(groups.values().map(ToOwned::to_owned).collect());
//     }

//     pub fn display(&self) {
//         let groups = self.groups.lock().unwrap();
//         println!("----------");
//         for (n, g) in &groups.node_replicas {
//             println!(
//                 "node replicas: {} -> {:?}",
//                 n,
//                 g.iter()
//                     .map(|r| format!("{}({})", r.0.id, r.1))
//                     .collect::<Vec<String>>()
//             )
//         }
//         let descs = &groups.descs;
//         for g in descs.values() {
//             let shards = g.shards.iter().map(|s| s.id).collect::<Vec<u64>>();
//             println!("group shards: {} -> {:?}", g.id, shards);
//         }

//         let nodes = self.nodes.lock().unwrap();
//         println!(
//             "cluster_nodes: {:?}",
//             nodes.iter().map(|n| n.id).collect::<Vec<u64>>()
//         );

//         let state = self.replicas.lock().unwrap();
//         let mut node_leaders: HashMap<u64, Vec<u64>> = HashMap::new();
//         let rs = state.values();
//         for r in rs.filter(|s| s.role == RaftRole::Leader as i32) {
//             let n = groups
//                 .descs
//                 .get(&r.group_id)
//                 .unwrap()
//                 .replicas
//                 .iter()
//                 .find(|d| d.id == r.replica_id)
//                 .unwrap()
//                 .node_id;
//             match node_leaders.entry(n) {
//                 Entry::Occupied(mut ent) => {
//                     ent.get_mut().push(r.group_id);
//                 }
//                 Entry::Vacant(ent) => {
//                     ent.insert(vec![r.group_id]);
//                 }
//             }
//         }
//         for (n, g) in node_leaders {
//             println!("node group leader: {} -> {:?}", n, g,)
//         }

//         println!("----------");
//     }
// }
