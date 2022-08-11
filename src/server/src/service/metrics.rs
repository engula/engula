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
use engula_api::server::v1::*;
use lazy_static::lazy_static;
use prometheus::*;
use prometheus_static_metric::make_static_metric;

make_static_metric! {
    pub struct GroupRequestTotal: IntCounter {
        "type" => {
            get,
            put,
            delete,
            list,
            transfer,
            batch_write,
            accept_shard,
            create_shard,
            move_replicas,
            change_replicas,
        }
    }
    pub struct GroupRequestDuration: Histogram {
        "type" => {
            get,
            put,
            delete,
            list,
            transfer,
            batch_write,
            accept_shard,
            create_shard,
            move_replicas,
            change_replicas,
        }
    }
}

// For group request
lazy_static! {
    pub static ref NODE_SERVICE_GROUP_REQUEST_TOTAL_VEC: IntCounterVec = register_int_counter_vec!(
        "node_service_group_request_total",
        "The total group requests of node service",
        &["type"]
    )
    .unwrap();
    pub static ref NODE_SERVICE_GROUP_REQUEST_TOTAL: GroupRequestTotal =
        GroupRequestTotal::from(&*NODE_SERVICE_GROUP_REQUEST_TOTAL_VEC);
    pub static ref NODE_SERVICE_GROUP_REQUEST_DURATION_SECONDS_VEC: HistogramVec =
        register_histogram_vec!(
            "node_service_group_request_duration_seconds",
            "The intervals of group requests of node service",
            &["type"]
        )
        .unwrap();
    pub static ref NODE_SERVICE_GROUP_REQUEST_DURATION_SECONDS: GroupRequestDuration =
        GroupRequestDuration::from(&*NODE_SERVICE_GROUP_REQUEST_DURATION_SECONDS_VEC);
}

pub fn take_group_request_metrics(request: &GroupRequest) -> Option<&'static Histogram> {
    use group_request_union::Request;

    match request.request.as_ref().and_then(|v| v.request.as_ref()) {
        Some(Request::Get(_)) => {
            NODE_SERVICE_GROUP_REQUEST_TOTAL.get.inc();
            Some(&NODE_SERVICE_GROUP_REQUEST_DURATION_SECONDS.get)
        }
        Some(Request::Put(_)) => {
            NODE_SERVICE_GROUP_REQUEST_TOTAL.put.inc();
            Some(&NODE_SERVICE_GROUP_REQUEST_DURATION_SECONDS.put)
        }
        Some(Request::Delete(_)) => {
            NODE_SERVICE_GROUP_REQUEST_TOTAL.delete.inc();
            Some(&NODE_SERVICE_GROUP_REQUEST_DURATION_SECONDS.delete)
        }
        Some(Request::PrefixList(_)) => {
            NODE_SERVICE_GROUP_REQUEST_TOTAL.list.inc();
            Some(&NODE_SERVICE_GROUP_REQUEST_DURATION_SECONDS.list)
        }
        Some(Request::BatchWrite(_)) => {
            NODE_SERVICE_GROUP_REQUEST_TOTAL.batch_write.inc();
            Some(&NODE_SERVICE_GROUP_REQUEST_DURATION_SECONDS.batch_write)
        }
        Some(Request::AcceptShard(_)) => {
            NODE_SERVICE_GROUP_REQUEST_TOTAL.accept_shard.inc();
            Some(&NODE_SERVICE_GROUP_REQUEST_DURATION_SECONDS.accept_shard)
        }
        Some(Request::CreateShard(_)) => {
            NODE_SERVICE_GROUP_REQUEST_TOTAL.create_shard.inc();
            Some(&NODE_SERVICE_GROUP_REQUEST_DURATION_SECONDS.create_shard)
        }
        Some(Request::ChangeReplicas(_)) => {
            NODE_SERVICE_GROUP_REQUEST_TOTAL.change_replicas.inc();
            Some(&NODE_SERVICE_GROUP_REQUEST_DURATION_SECONDS.change_replicas)
        }
        Some(Request::Transfer(_)) => {
            NODE_SERVICE_GROUP_REQUEST_TOTAL.transfer.inc();
            Some(&NODE_SERVICE_GROUP_REQUEST_DURATION_SECONDS.transfer)
        }
        Some(Request::MoveReplicas(_)) => {
            NODE_SERVICE_GROUP_REQUEST_TOTAL.move_replicas.inc();
            Some(&NODE_SERVICE_GROUP_REQUEST_DURATION_SECONDS.move_replicas)
        }
        None => None,
    }
}

// For batch request.
lazy_static! {
    pub static ref NODE_SERVICE_BATCH_REQUEST_TOTAL: IntCounter = register_int_counter!(
        "node_service_batch_request_total",
        "The total batch requests of node service",
    )
    .unwrap();
    pub static ref NODE_SERVICE_BATCH_REQUEST_SIZE: Histogram = register_histogram!(
        "node_service_batch_request_size",
        "The batch size of batch requests of node service",
    )
    .unwrap();
    pub static ref NODE_SERVICE_BATCH_REQUEST_DURATION_SECONDS: Histogram = register_histogram!(
        "node_service_batch_request_duration_seconds",
        "The intervals of batch requests of node service",
    )
    .unwrap();
}

pub fn take_batch_request_metrics(request: &BatchRequest) -> &'static Histogram {
    NODE_SERVICE_BATCH_REQUEST_SIZE.observe(request.requests.len() as f64);
    NODE_SERVICE_BATCH_REQUEST_TOTAL.inc();
    &*NODE_SERVICE_BATCH_REQUEST_DURATION_SECONDS
}

macro_rules! simple_node_method {
    ($name: ident) => {
        paste::paste! {
            lazy_static! {
                pub static ref [<NODE_SERVICE_ $name:upper _REQUEST_TOTAL>]: IntCounter = register_int_counter!(
                    concat!("node_service_", stringify!($name), "_request_total"),
                    concat!("The total ", stringify!($name), " requests of node service")
                )
                .unwrap();
                pub static ref [<NODE_SERVICE_ $name:upper _REQUEST_DURATION_SECONDS>]: Histogram =
                    register_histogram!(
                        concat!("node_service_", stringify!($name), "_request_duration_seconds"),
                        concat!("The intervals of ", stringify!($name), " requests of node service"),
                    )
                    .unwrap();
            }

            pub fn [<take_ $name _request_metrics>]() -> &'static Histogram {
                [<NODE_SERVICE_ $name:upper _REQUEST_TOTAL>].inc();
                &*[<NODE_SERVICE_ $name:upper _REQUEST_DURATION_SECONDS>]
            }
        }
    };
}

simple_node_method!(get_root);
simple_node_method!(create_replica);
simple_node_method!(remove_replica);
simple_node_method!(root_heartbeat);
simple_node_method!(migrate);
simple_node_method!(pull);
simple_node_method!(forward);

macro_rules! simple_root_method {
    ($name: ident) => {
        paste::paste! {
            lazy_static! {
                pub static ref [<ROOT_SERVICE_ $name:upper _REQUEST_TOTAL>]: IntCounter = register_int_counter!(
                    concat!("root_service_", stringify!($name), "_request_total"),
                    concat!("The total ", stringify!($name), " requests of root service")
                )
                .unwrap();
                pub static ref [<ROOT_SERVICE_ $name:upper _REQUEST_DURATION_SECONDS>]: Histogram =
                    register_histogram!(
                        concat!("root_service_", stringify!($name), "_request_duration_seconds"),
                        concat!("The intervals of ", stringify!($name), " requests of root service"),
                    )
                    .unwrap();
            }

            pub fn [<take_ $name _request_metrics>]() -> &'static Histogram {
                [<ROOT_SERVICE_ $name:upper _REQUEST_TOTAL>].inc();
                &*[<ROOT_SERVICE_ $name:upper _REQUEST_DURATION_SECONDS>]
            }
        }
    };
}

simple_root_method!(report);
simple_root_method!(watch);
simple_root_method!(admin);
simple_root_method!(join);
simple_root_method!(alloc_replica);

lazy_static! {
    pub static ref RAFT_SERVICE_MSG_REQUEST_TOTAL: IntCounter = register_int_counter!(
        "raft_service_msg_request_total",
        "The total msg requests of raft service",
    )
    .unwrap();
    pub static ref RAFT_SERVICE_SNAPSHOT_REQUEST_TOTAL: IntCounter = register_int_counter!(
        "raft_service_snapshot_request_total",
        "The total snapshot requests of raft service",
    )
    .unwrap();
    pub static ref RAFT_SERVICE_MSG_BATCH_SIZE: Histogram = register_histogram!(
        "raft_service_msg_batch_size",
        "The batch size of msg requests of raft service",
    )
    .unwrap();
}

#[macro_export]
macro_rules! record_latency {
    ($metrics: expr) => {
        let _timer = $metrics.start_timer();
    };
}

#[macro_export]
macro_rules! record_latency_opt {
    ($metrics_opt: expr) => {
        let _timer = $metrics_opt.map(|m| m.start_timer());
    };
}
