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
use prometheus_static_metric::make_static_metric;

// root status.
lazy_static! {
    pub static ref LEADER_STATE: IntGauge = register_int_gauge!(
        "root_service_node_as_leader_gauge",
        "the node as root leader count"
    )
    .unwrap();
}

// bootstrap root.

lazy_static! {
    pub static ref BOOTSTRAP_DURATION_SECONDS: Histogram = register_histogram!(
        "root_bootstrap_duration_seconds",
        "the duration of bootstrap root service"
    )
    .unwrap();
    pub static ref BOOTSTRAP_FAIL_COUNT: IntCounter = register_int_counter!(
        "root_boostrap_fail_count",
        "the count of boostrap root fail"
    )
    .unwrap();
}

// reconcile.

make_static_metric! {
    pub struct ReconcileScheduleHandleTaskTotal: IntCounter {
        "type" => {
            create_group,
            reallocate_replica,
            migrate_shard,
            transfer_leader,
            create_collection_shards,
            shed_group_leaders,
            shed_root_leader,
        }
    }
    pub struct ReconcileScheduleHandleTaskDuration: Histogram {
        "type" => {
            create_group,
            reallocate_replica,
            migrate_shard,
            transfer_leader,
            create_collection_shards,
            shed_group_leaders,
            shed_root_leader,
        }
    }
    pub struct ReconcileScheduleCreateGroupStepDuration: Histogram {
        "type" => {
            init,
            create,
            rollback,
            finish,
        }
    }
    pub struct ReconcileScheduleReallocateReplicaStepDuration: Histogram {
        "type" => {
            create_dest_replica,
            add_dest_learner,
            replica_dest_voter,
            shed_src_leader,
            remove_src_membership,
            remove_src_replica,
            finish,
        }
    }
    pub struct ReconcileScheduleCreateCollectionStepDuration: Histogram {
        "type" => {
            create,
            rollback,
        }
    }
}

lazy_static! {
    pub static ref RECONCILE_STEP_DURATION_SECONDS: Histogram = register_histogram!(
        "root_reconcile_step_duration_seconds",
        "the duration of one reconcile step"
    )
    .unwrap();
    pub static ref RECONCILE_ALREADY_BALANCED_TOTAL: Counter = register_counter!(
        "root_reconcile_already_balanced_total",
        "the count of observe cluster is already balanced in root reconcile task"
    )
    .unwrap();
    pub static ref RECONCILE_SCHEDULER_TASK_QUEUE_SIZE: Histogram = register_histogram!(
        "root_reconcile_scheduler_task_queue_size",
        "the size of scheduler task queue size during each reconcile step"
    )
    .unwrap();
    pub static ref RECONCILE_HANDLE_TASK_TOTAL_VEC: IntCounterVec = register_int_counter_vec!(
        "root_reconcile_scheduler_task_handle_total",
        "The total handle count of root reconcile scheduler",
        &["type"]
    )
    .unwrap();
    pub static ref RECONCILE_HANDLE_TASK_TOTAL: ReconcileScheduleHandleTaskTotal =
        ReconcileScheduleHandleTaskTotal::from(&RECONCILE_HANDLE_TASK_TOTAL_VEC);
    pub static ref RECONCILE_CHECK_DURATION: Histogram = register_histogram!(
        "root_reconcile_scheduler_check_task_duration",
        "the duration of scheduler check and prepare tasks",
    )
    .unwrap();
    pub static ref RECONCILE_HANDL_TASK_DURATION_VEC: HistogramVec = register_histogram_vec!(
        "root_reconcile_scheduler_task_handle_duration",
        "the total handle duration of root reconcile scheduler",
        &["type"]
    )
    .unwrap();
    pub static ref RECONCILE_HANDL_TASK_DURATION: ReconcileScheduleHandleTaskDuration =
        ReconcileScheduleHandleTaskDuration::from(&RECONCILE_HANDL_TASK_DURATION_VEC);
    pub static ref RECONCILE_CREATE_GROUP_STEP_DURATION_VEC: HistogramVec =
        register_histogram_vec!(
            "root_reconcile_scheduler_create_group_step_duration",
            "the step create_group handle duration of root reconcile scheduler",
            &["type"]
        )
        .unwrap();
    pub static ref RECONCILE_CREATE_GROUP_STEP_DURATION: ReconcileScheduleCreateGroupStepDuration =
        ReconcileScheduleCreateGroupStepDuration::from(&RECONCILE_CREATE_GROUP_STEP_DURATION_VEC);
    pub static ref RECONCILE_REALLOCATE_REPLICA_STEP_DURATION_VEC: HistogramVec =
        register_histogram_vec!(
            "root_reconcile_scheduler_reallocate_replica_step_duration",
            "the step reallocate replica handle duration of root reconcile scheduler",
            &["type"]
        )
        .unwrap();
    pub static ref RECONCILE_REALLOCATE_REPLICA_STEP_DURATION: ReconcileScheduleReallocateReplicaStepDuration =
        ReconcileScheduleReallocateReplicaStepDuration::from(
            &RECONCILE_REALLOCATE_REPLICA_STEP_DURATION_VEC
        );
    pub static ref RECONCILE_CREATE_COLLECTION_STEP_DURATION_VEC: HistogramVec =
        register_histogram_vec!(
            "root_reconcile_scheduler_create_collection_step_duration",
            "the step create_ collection shards handle duration of root reconcile scheduler",
            &["type"]
        )
        .unwrap();
    pub static ref RECONCILE_CREATE_COLLECTION_STEP_DURATION: ReconcileScheduleCreateCollectionStepDuration =
        ReconcileScheduleCreateCollectionStepDuration::from(
            &RECONCILE_CREATE_COLLECTION_STEP_DURATION_VEC
        );
    pub static ref RECONCILE_RETRYL_TASK_TOTAL_VEC: IntCounterVec = register_int_counter_vec!(
        "root_reconcile_scheduler_task_retry_total",
        "The total retry count of root reconcile scheduler",
        &["type"]
    )
    .unwrap();
    pub static ref RECONCILE_RETRYL_TASK_TOTAL: ReconcileScheduleHandleTaskTotal =
        ReconcileScheduleHandleTaskTotal::from(&RECONCILE_RETRYL_TASK_TOTAL_VEC);
}

// hearbeat & report

make_static_metric! {
    pub struct UpdateGroupDesc: IntCounter {
        "type" => {
            report,
            heartbeat,
        }
    }
    pub struct UpdateReplicaState: IntCounter {
        "type" => {
            report,
            heartbeat,
        }
    }
}

lazy_static! {
    pub static ref HEARTBEAT_STEP_DURATION_SECONDS: Histogram = register_histogram!(
        "root_heartbeat_step_duration_seconds",
        "the duration of one heartbeat step"
    )
    .unwrap();
    pub static ref HEARTBEAT_TASK_QUEUE_SIZE: Histogram = register_histogram!(
        "root_heartbeat_task_queue_size",
        "the size of heartbeat task queue size during each heartbeat step observered"
    )
    .unwrap();
    pub static ref HEARTBEAT_TASK_FAIL_TOTAL: IntCounterVec = register_int_counter_vec!(
        "root_heartbeat_fail_total",
        "the count of heartbeat fail",
        &["node"]
    )
    .unwrap();
    pub static ref HEARTBEAT_RESCHEDULE_EARLY_INTERVAL_SECONDS: Histogram = register_histogram!(
        "root_heartbeat_reschedule_early_interval_seconds",
        "the interval of heartbeat be rescheduled early"
    )
    .unwrap();
    pub static ref HEARTBEAT_NODES_RPC_DURATION_SECONDS: Histogram = register_histogram!(
        "root_heartbeat_rpc_nodes_duration_seconds",
        "the duration of rpc heartbeat multiple nodes togather"
    )
    .unwrap();
    pub static ref HEARTBEAT_NODES_BATCH_SIZE: Histogram = register_histogram!(
        "root_heartbeat_nodes_batch_size",
        "the number of nodes be sent in one heartbeat step"
    )
    .unwrap();
    pub static ref HEARTBEAT_HANDLE_GROUP_DETAIL_DURATION_SECONDS: Histogram = register_histogram!(
        "root_heartbeat_handle_group_detail_seconds",
        "the duration of handle update group detail after recieve heartbeat response"
    )
    .unwrap();
    pub static ref HEARTBEAT_HANDLE_NODE_STATS_DURATION_SECONDS: Histogram = register_histogram!(
        "root_heartbeat_handle_node_stats_seconds",
        "the duration of handle update stats after recieve heartbeat response"
    )
    .unwrap();
    pub static ref HEARTBEAT_UPDATE_NODE_STATS_TOTAL: IntCounter = register_int_counter!(
        "root_heartbeat_update_node_stats_total",
        "the count of real update node stats after recieve heartbeat response",
    )
    .unwrap();
    pub static ref ROOT_UPDATE_GROUP_DESC_TOTAL_VEC: IntCounterVec = register_int_counter_vec!(
        "root_update_group_desc_total",
        "The count of update group_desc",
        &["type"]
    )
    .unwrap();
    pub static ref ROOT_UPDATE_GROUP_DESC_TOTAL: UpdateGroupDesc =
        UpdateGroupDesc::from(&ROOT_UPDATE_GROUP_DESC_TOTAL_VEC);
    pub static ref ROOT_UPDATE_REPLICA_STATE_TOTAL_VEC: IntCounterVec = register_int_counter_vec!(
        "root_update_replica_state_total",
        "The count of update replica_state",
        &["type"]
    )
    .unwrap();
    pub static ref ROOT_UPDATE_REPLICA_STATE_TOTAL: UpdateReplicaState =
        UpdateReplicaState::from(&ROOT_UPDATE_REPLICA_STATE_TOTAL_VEC);
}

// watch
lazy_static! {
    pub static ref WATCH_TABLE_SIZE: IntGauge =
        register_int_gauge!("root_watch_table_size", "the count of the root watcher").unwrap();
    pub static ref WATCH_NOTIFY_DURATION_SECONDS: Histogram = register_histogram!(
        "root_watch_notify_duration_seconds",
        "the duration of watch notify(mainly wait watch lock)",
    )
    .unwrap();
}
