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

use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use engula_api::server::v1::NodeStatus;
use prometheus::{core::Collector, *};
use prometheus_static_metric::make_static_metric;

use crate::Server;

make_static_metric! {
    struct NodeTotal: IntGauge {
        "type" => {
            all,
            alive,
            schedulable,
        }
    }
}

pub struct RootCollector {
    shared: Arc<RootCollectorShared>,
}

pub struct RootCollectorShared {
    server: Server,
    descs: Vec<core::Desc>,

    is_root_leader: AtomicBool,

    nodes: NodeTotal,
    groups: IntGauge,
    replica_counts: IntGaugeVec,
    leader_counts: IntGaugeVec,
    shard_counts: IntGaugeVec,
}

impl RootCollectorShared {
    pub fn new(namespace: &str, server: Server) -> Self {
        let mut descs = Vec::new();
        let nodes = {
            let nodes_vec = IntGaugeVec::new(
                Opts::new("cluster_node_total", "total nodes in cluster.").namespace(namespace),
                &["type"],
            )
            .unwrap();
            descs.extend(nodes_vec.desc().into_iter().cloned());
            NodeTotal::from(&nodes_vec)
        };
        let groups = IntGauge::with_opts(
            Opts::new("cluster_group_total", "total groups in cluster.").namespace(namespace),
        )
        .unwrap();
        descs.extend(groups.desc().into_iter().cloned());
        let replica_counts = IntGaugeVec::new(
            Opts::new("cluster_node_replica_total", "replica count for each node"),
            &["node"],
        )
        .unwrap();
        descs.extend(replica_counts.desc().into_iter().cloned());
        let leader_counts = IntGaugeVec::new(
            Opts::new("cluster_node_leader_total", "leader count for each node"),
            &["node"],
        )
        .unwrap();
        descs.extend(leader_counts.desc().into_iter().cloned());
        let shard_counts = IntGaugeVec::new(
            Opts::new("cluster_group_shard_total", "shard count for each group"),
            &["group"],
        )
        .unwrap();
        descs.extend(shard_counts.desc().into_iter().cloned());
        let is_root_leader = false.into();
        Self {
            server,
            descs,
            nodes,
            groups,
            replica_counts,
            leader_counts,
            shard_counts,
            is_root_leader,
        }
    }

    pub async fn try_refresh(&self) {
        let root = self.server.root.to_owned();
        if let Ok(info) = root.info().await {
            self.is_root_leader.store(true, Ordering::Relaxed);
            // nodes count & status.
            self.nodes.all.set(info.nodes.len() as i64);
            self.nodes.alive.set(
                info.nodes
                    .iter()
                    .filter(|n| {
                        !matches!(
                            NodeStatus::from_i32(n.status).unwrap(),
                            NodeStatus::Decommissioning | NodeStatus::Decommissioned
                        )
                    })
                    .count() as i64,
            );
            self.nodes.schedulable.set(
                info.nodes
                    .iter()
                    .filter(|n| {
                        matches!(NodeStatus::from_i32(n.status).unwrap(), NodeStatus::Active)
                    })
                    .count() as i64,
            );

            // groups count.
            self.groups.set(info.groups.len() as i64);
            self.shard_counts.reset();
            for g in &info.groups {
                self.shard_counts
                    .with_label_values(&[&g.id.to_string()])
                    .set(g.shards.len() as i64);
            }

            // replica cnt & leader cnt.
            self.replica_counts.reset();
            self.leader_counts.reset();
            for n in &info.nodes {
                self.replica_counts
                    .with_label_values(&[&n.id.to_string()])
                    .set(n.replicas.len() as i64);
                self.leader_counts
                    .with_label_values(&[&n.id.to_string()])
                    .set(n.leaders.len() as i64);
            }
        } else {
            self.is_root_leader.store(false, Ordering::Relaxed);
        }
    }
}

impl RootCollector {
    pub fn new(shared: Arc<RootCollectorShared>) -> Self {
        Self { shared }
    }
}

impl core::Collector for RootCollector {
    fn desc(&self) -> Vec<&core::Desc> {
        self.shared.descs.iter().collect()
    }

    fn collect(&self) -> Vec<proto::MetricFamily> {
        let mut mfs = Vec::new();
        if self.shared.is_root_leader.load(Ordering::Relaxed) {
            mfs.extend(self.shared.nodes.all.collect());
            mfs.extend(self.shared.nodes.alive.collect());
            mfs.extend(self.shared.nodes.schedulable.collect());
            mfs.extend(self.shared.groups.collect());
            mfs.extend(self.shared.replica_counts.collect());
            mfs.extend(self.shared.leader_counts.collect());
            mfs.extend(self.shared.shard_counts.collect());
        }
        mfs
    }
}
