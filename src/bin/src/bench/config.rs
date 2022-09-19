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

use std::time::Duration;

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AppConfig {
    pub num_threads: usize,
    pub report_interval: Duration,
    pub operation: usize,

    pub addrs: Vec<String>,

    pub database: String,
    pub collection: String,
    pub num_shards: u32,
    pub create_if_missing: bool,

    pub data: DataConfig,
    pub key: KeyConfig,
    pub worker: WorkerConfig,
}

impl Default for AppConfig {
    fn default() -> Self {
        AppConfig {
            num_threads: num_cpus::get(),
            report_interval: Duration::from_secs(10),
            operation: 100000,
            addrs: vec!["0.0.0.0:21805".into()],
            database: "db".into(),
            collection: "table".into(),
            num_shards: 64,
            create_if_missing: true,
            data: DataConfig::default(),
            key: KeyConfig::default(),
            worker: WorkerConfig::default(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DataConfig {
    pub inserted: u64,
    pub limited: u64,

    pub read: f64,
    pub write: f64,
    pub value: std::ops::Range<usize>,
}

impl Default for DataConfig {
    fn default() -> Self {
        DataConfig {
            inserted: 10000,
            limited: 10000,
            read: 0.5,
            write: 0.5,
            value: 10..11,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct KeyConfig {
    pub prefix: String,
    pub leading: usize,
}

impl Default for KeyConfig {
    fn default() -> Self {
        KeyConfig {
            prefix: "user_".to_owned(),
            leading: 10,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WorkerConfig {
    pub num_worker: usize,
    pub start_intervals: Option<Duration>,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        WorkerConfig {
            num_worker: 1,
            start_intervals: None,
        }
    }
}
