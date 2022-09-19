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

use std::time::Instant;

use lazy_static::lazy_static;
use prometheus::*;
use prometheus_static_metric::make_static_metric;

use super::ReadPolicy;
use crate::{Error, Result};

make_static_metric! {
    struct ProposeTotal: IntCounter {
        "type" => {
            ok,
            busy,
            not_leader,
            unknown,
        }
    }
    struct ProposeDuration: Histogram {
        "type" => {
            ok,
            busy,
            not_leader,
            unknown,
        }
    }
    struct ReadTotal: IntCounter {
        "type" => {
            lease_based,
            read_index,
        }
    }
    struct ReadDuration: Histogram {
        "type" => {
            lease_based,
            read_index,
        }
    }
}

lazy_static! {
    pub static ref RAFTGROUP_PROPOSE_TOTAL_VEC: IntCounterVec = register_int_counter_vec!(
        "raftgroup_propose_total",
        "The total of propose invoked of raftgroup",
        &["type"],
    )
    .unwrap();
    pub static ref RAFTGROUP_PROPOSE_TOTAL: ProposeTotal =
        ProposeTotal::from(&RAFTGROUP_PROPOSE_TOTAL_VEC);
}

lazy_static! {
    pub static ref RAFTGROUP_PROPOSE_DURATION_SECONDS_VEC: HistogramVec = register_histogram_vec!(
        "raftgroup_propose_duration_seconds",
        "The intervals of propose of raftgroup",
        &["type"],
        exponential_buckets(0.00005, 1.8, 26).unwrap()
    )
    .unwrap();
    pub static ref RAFTGROUP_PROPOSE_DURATION_SECONDS: ProposeDuration =
        ProposeDuration::from(&RAFTGROUP_PROPOSE_DURATION_SECONDS_VEC);
}

lazy_static! {
    pub static ref RAFTGROUP_READ_TOTAL_VEC: IntCounterVec = register_int_counter_vec!(
        "raftgroup_read_total",
        "The total of read of raftgroup",
        &["type"]
    )
    .unwrap();
    pub static ref RAFTGROUP_READ_TOTAL: ReadTotal = ReadTotal::from(&RAFTGROUP_READ_TOTAL_VEC);
}

lazy_static! {
    pub static ref RAFTGROUP_READ_DURATION_SECONDS_VEC: HistogramVec = register_histogram_vec!(
        "raftgroup_read_duration_seconds",
        "The intervals of read of raftgroup",
        &["type"],
        exponential_buckets(0.00005, 1.8, 26).unwrap()
    )
    .unwrap();
    pub static ref RAFTGROUP_READ_DURATION_SECONDS: ReadDuration =
        ReadDuration::from(&RAFTGROUP_READ_DURATION_SECONDS_VEC);
}

lazy_static! {
    pub static ref RAFTGROUP_CONFIG_CHANGE_TOTAL: IntCounter = register_int_counter!(
        "raftgroup_config_change_total",
        "The total of config change of raftgroup",
    )
    .unwrap();
    pub static ref RAFTGROUP_TRANSFER_LEADER_TOTAL: IntCounter = register_int_counter!(
        "raftgroup_transfer_leader_total",
        "The total of transfer leader of raftgroup",
    )
    .unwrap();
    pub static ref RAFTGROUP_UNREACHABLE_TOTAL: IntCounter = register_int_counter!(
        "raftgroup_unreachable_total",
        "The total of unreachable of raftgroup",
    )
    .unwrap();
}

lazy_static! {
    pub static ref RAFTGROUP_SEND_SNAPSHOT_TOTAL: IntCounter = register_int_counter!(
        "raftgroup_send_snapshot_total",
        "The total of send snapshot of raftgroup",
    )
    .unwrap();
    pub static ref RAFTGROUP_SEND_SNAPSHOT_BYTES_TOTAL: IntCounter = register_int_counter!(
        "raftgroup_send_snapshot_bytes_total",
        "The total bytes of send snapshot of raftgroup",
    )
    .unwrap();
}

lazy_static! {
    pub static ref RAFTGROUP_DOWNLOAD_SNAPSHOT_TOTAL: IntCounter = register_int_counter!(
        "raftgroup_download_snapshot_total",
        "The total of download snapshot of raftgroup",
    )
    .unwrap();
    pub static ref RAFTGROUP_DOWNLOAD_SNAPSHOT_BYTES_TOTAL: IntCounter = register_int_counter!(
        "raftgroup_download_snapshot_bytes_total",
        "The total bytes of download snapshot of raftgroup",
    )
    .unwrap();
    pub static ref RAFTGROUP_DOWNLOAD_SNAPSHOT_DURATION_SECONDS: Histogram = register_histogram!(
        "raftgroup_download_snapshot_duration_seconds",
        "The intervals of download snapshot of raftgroup",
        exponential_buckets(0.005, 1.8, 22).unwrap(),
    )
    .unwrap();
}

lazy_static! {
    pub static ref RAFTGROUP_APPLY_SNAPSHOT_TOTAL: IntCounter = register_int_counter!(
        "raftgroup_apply_snapshot_total",
        "The total of apply snapshot of raftgroup",
    )
    .unwrap();
    pub static ref RAFTGROUP_APPLY_SNAPSHOT_DURATION_SECONDS: Histogram = register_histogram!(
        "raftgroup_apply_snapshot_duration_seconds",
        "The intervals of apply snapshot of raftgroup",
        exponential_buckets(0.005, 1.8, 22).unwrap(),
    )
    .unwrap();
}

lazy_static! {
    pub static ref RAFTGROUP_CREATE_SNAPSHOT_TOTAL: IntCounter = register_int_counter!(
        "raftgroup_create_snapshot_total",
        "The total of create snapshot of raftgroup",
    )
    .unwrap();
    pub static ref RAFTGROUP_CREATE_SNAPSHOT_DURATION_SECONDS: Histogram = register_histogram!(
        "raftgroup_create_snapshot_duration_seconds",
        "The intervals of create snapshot of raftgroup",
        exponential_buckets(0.005, 1.8, 22).unwrap(),
    )
    .unwrap();
}

lazy_static! {
    pub static ref RAFTGROUP_WORKER_ADVANCE_TOTAL: IntCounter = register_int_counter!(
        "raftgroup_worker_advance_total",
        "The total of worker advance of raftgroup",
    )
    .unwrap();
    pub static ref RAFTGROUP_WORKER_ADVANCE_DURATION_SECONDS: Histogram = register_histogram!(
        "raftgroup_worker_advance_duration_seconds",
        "The intervals of worker advance of raftgroup",
        exponential_buckets(0.00005, 1.8, 26).unwrap()
    )
    .unwrap();
    pub static ref RAFTGROUP_WORKER_REQUEST_IN_QUEUE_DURATION_SECONDS: Histogram =
        register_histogram!(
            "raftgroup_worker_request_in_queue_duration_seconds",
            "The intervals of worker request in queue",
            exponential_buckets(0.00005, 1.8, 26).unwrap()
        )
        .unwrap();
    pub static ref RAFTGROUP_WORKER_COMPACT_LOG_DURATION_SECONDS: Histogram = register_histogram!(
        "raftgroup_worker_compact_log_duration_seconds",
        "The intervals of worker compact log of raftgroup",
        exponential_buckets(0.00005, 1.8, 26).unwrap()
    )
    .unwrap();
    pub static ref RAFTGROUP_WORKER_APPLY_DURATION_SECONDS: Histogram = register_histogram!(
        "raftgroup_worker_apply_duration_seconds",
        "The intervals of worker apply of raftgroup",
        exponential_buckets(0.00005, 1.8, 26).unwrap()
    )
    .unwrap();
    pub static ref RAFTGROUP_WORKER_CONSUME_REQUESTS_DURATION_SECONDS: Histogram =
        register_histogram!(
            "raftgroup_worker_consume_requests_duration_seconds",
            "The intervals of worker consume requests",
            exponential_buckets(0.00005, 1.8, 26).unwrap()
        )
        .unwrap();
    pub static ref RAFTGROUP_WORKER_ACCUMULATED_BYTES_SIZE: Histogram = register_histogram!(
        "raftgroup_worker_accumulated_bytes_size",
        "THe accumulated bytes size of each batching of raft worker",
        exponential_buckets(256.0, 1.8, 22).unwrap(),
    )
    .unwrap();
    pub static ref RAFTGROUP_WORKER_APPLY_ENTRIES_SIZE: Histogram = register_histogram!(
        "raftgroup_worker_apply_entries_size",
        "The size of entries to apply",
        exponential_buckets(1.0, 1.8, 22).unwrap(),
    )
    .unwrap();
}

pub fn take_read_metrics(read_policy: ReadPolicy) -> &'static Histogram {
    match read_policy {
        ReadPolicy::LeaseRead => {
            RAFTGROUP_READ_TOTAL.lease_based.inc();
            &RAFTGROUP_READ_DURATION_SECONDS.lease_based
        }
        ReadPolicy::ReadIndex => {
            RAFTGROUP_READ_TOTAL.read_index.inc();
            &RAFTGROUP_READ_DURATION_SECONDS.read_index
        }
        ReadPolicy::Relaxed => unreachable!(),
    }
}

pub fn take_propose_metrics(start_at: Instant, result: Result<()>) -> Result<()> {
    let elapsed = elapsed_seconds(start_at);
    match &result {
        Ok(()) => {
            RAFTGROUP_PROPOSE_TOTAL.ok.inc();
            RAFTGROUP_PROPOSE_DURATION_SECONDS.ok.observe(elapsed);
        }
        Err(Error::NotLeader(..)) => {
            RAFTGROUP_PROPOSE_TOTAL.not_leader.inc();
            RAFTGROUP_PROPOSE_DURATION_SECONDS
                .not_leader
                .observe(elapsed);
        }
        Err(Error::ServiceIsBusy(_)) => {
            RAFTGROUP_PROPOSE_TOTAL.busy.inc();
            RAFTGROUP_PROPOSE_DURATION_SECONDS.busy.observe(elapsed);
        }
        _ => {
            RAFTGROUP_PROPOSE_TOTAL.unknown.inc();
            RAFTGROUP_PROPOSE_DURATION_SECONDS.unknown.observe(elapsed);
        }
    }
    result
}

pub fn take_download_snapshot_metrics() -> &'static Histogram {
    RAFTGROUP_DOWNLOAD_SNAPSHOT_TOTAL.inc();
    &RAFTGROUP_DOWNLOAD_SNAPSHOT_DURATION_SECONDS
}

pub fn take_create_snapshot_metrics() -> &'static Histogram {
    RAFTGROUP_CREATE_SNAPSHOT_TOTAL.inc();
    &RAFTGROUP_CREATE_SNAPSHOT_DURATION_SECONDS
}

pub fn take_apply_snapshot_metrics() -> &'static Histogram {
    RAFTGROUP_APPLY_SNAPSHOT_TOTAL.inc();
    &RAFTGROUP_APPLY_SNAPSHOT_DURATION_SECONDS
}

pub fn elapsed_seconds(instant: Instant) -> f64 {
    let d = instant.elapsed();
    d.as_secs() as f64 + (d.subsec_nanos() as f64) / 1e9
}
