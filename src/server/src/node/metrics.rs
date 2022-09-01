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

use lazy_static::lazy_static;
use prometheus::*;

lazy_static! {
    pub static ref NODE_RETRY_TOTAL: IntCounter =
        register_int_counter!("node_retry_total", "The total retries of node",).unwrap();
    pub static ref NODE_DESTORY_REPLICA_TOTAL: IntCounter = register_int_counter!(
        "node_destory_replica_total",
        "The total destory replica of node"
    )
    .unwrap();
    pub static ref NODE_DESTORY_REPLICA_DURATION_SECONDS: Histogram = register_histogram!(
        "node_destory_replica_duration_seconds",
        "The intervals of destory replica of node",
        exponential_buckets(0.005, 1.8, 22).unwrap(),
    )
    .unwrap();
    pub static ref NODE_REPORT_TOTAL: IntCounter =
        register_int_counter!("node_report_total", "The total reports of node").unwrap();
    pub static ref NODE_REPORT_DURATION_SECONDS: Histogram = register_histogram!(
        "node_report_duration_seconds",
        "The intervals of report of node",
        exponential_buckets(0.00005, 1.8, 26).unwrap(),
    )
    .unwrap();
    pub static ref NODE_PULL_SHARD_TOTAL: IntCounter =
        register_int_counter!("node_pull_shard_total", "The total of pull shards of node").unwrap();
    pub static ref NODE_PULL_SHARD_DURATION_SECONDS: Histogram = register_histogram!(
        "node_pull_shard_duration_seconds",
        "The intervals of pull shard of node",
        exponential_buckets(0.00005, 1.8, 26).unwrap(),
    )
    .unwrap();
}

pub fn take_destory_replica_metrics() -> &'static Histogram {
    NODE_DESTORY_REPLICA_TOTAL.inc();
    &NODE_DESTORY_REPLICA_DURATION_SECONDS
}

pub fn take_report_metrics() -> &'static Histogram {
    NODE_REPORT_TOTAL.inc();
    &NODE_REPORT_DURATION_SECONDS
}

pub fn take_pull_shard_metrics() -> &'static Histogram {
    NODE_PULL_SHARD_TOTAL.inc();
    &NODE_PULL_SHARD_DURATION_SECONDS
}
