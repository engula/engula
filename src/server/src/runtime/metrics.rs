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

use super::TaskPriority;

make_static_metric! {
    struct SpawnTotal: IntCounter {
        "priority" => {
            real,
            high,
            middle,
            low,
            io_high,
            io_low,
        }
    }
}

lazy_static! {
    pub static ref EXECUTOR_PARK_TOTAL: IntCounter =
        register_int_counter!("executor_park_total", "The total of park() of executor",).unwrap();
    pub static ref EXECUTOR_UNPARK_TOTAL: IntCounter =
        register_int_counter!("executor_unpark_total", "The total of unpark() of executor",)
            .unwrap();
    pub static ref EXECUTOR_SPAWN_TOTAL_VEC: IntCounterVec = register_int_counter_vec!(
        "executor_spawn_total",
        "The total of spawn of executor",
        &["priority"],
    )
    .unwrap();
    pub static ref EXECUTOR_SPAWN_TOTAL: SpawnTotal = SpawnTotal::from(&EXECUTOR_SPAWN_TOTAL_VEC);
    pub static ref EXECUTOR_TASK_FIRST_POLL_DURATION_SECONDS: Histogram = register_histogram!(
        "executor_task_first_poll_duration_seconds",
        "The interval between spawn and first poll of a task",
        exponential_buckets(0.00005, 1.8, 26).unwrap(),
    )
    .unwrap();
    pub static ref EXECUTOR_TASK_POLL_DURATION_SECONDS: Histogram = register_histogram!(
        "executor_task_poll_duration_seconds",
        "The interval of poll of a task",
        exponential_buckets(0.00005, 1.8, 26).unwrap(),
    )
    .unwrap();
    pub static ref EXECUTOR_TASK_EXECUTE_DURATION_SECONDS: Histogram = register_histogram!(
        "executor_task_execute_duration_seconds",
        "The interval of execution of a task",
        exponential_buckets(0.00005, 1.8, 26).unwrap(),
    )
    .unwrap();
}

#[inline]
pub fn take_spawn_metrics(priority: TaskPriority) {
    match priority {
        TaskPriority::Real => EXECUTOR_SPAWN_TOTAL.real.inc(),
        TaskPriority::High => EXECUTOR_SPAWN_TOTAL.high.inc(),
        TaskPriority::Middle => EXECUTOR_SPAWN_TOTAL.middle.inc(),
        TaskPriority::Low => EXECUTOR_SPAWN_TOTAL.low.inc(),
        TaskPriority::IoHigh => EXECUTOR_SPAWN_TOTAL.io_high.inc(),
        TaskPriority::IoLow => EXECUTOR_SPAWN_TOTAL.io_low.inc(),
    }
}
