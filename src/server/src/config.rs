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

use engula_engine::DiskOptions;
use serde::Deserialize;

#[derive(Debug)]
pub struct Config {
    pub addr: String,
    pub connection_timeout: Option<Duration>,
    pub max_memory: usize,
    pub root: String,
    pub disk_opts: DiskOptions,
}

#[derive(Deserialize, Debug)]
pub struct ConfigBuilder {
    pub addr: Option<String>,
    pub timeout: Option<u64>,
    pub max_memory: Option<usize>,
    pub root: Option<String>,
    pub disk_opts: DiskOptions,
}

impl Default for ConfigBuilder {
    fn default() -> Self {
        Self {
            addr: Some("127.0.0.1:21716".to_string()),
            timeout: Some(0),
            max_memory: Some(0),
            root: None,
            disk_opts: DiskOptions {
                mem_capacity: 128 * 1024 * 1204,
                file_size: 32 * 1024 * 1024,
                disk_capacity: 1024 * 1024 * 1024,
                write_buffer_size: 64 * 1024 * 1024,
            },
        }
    }
}

impl ConfigBuilder {
    pub fn build(self) -> Config {
        Config {
            addr: self.addr.expect("address of the server is required"),
            connection_timeout: self.timeout.and_then(|timeout| match timeout {
                0 => None,
                timeout => Some(Duration::from_secs(timeout)),
            }),
            max_memory: self.max_memory.expect("max memory is required"),
            root: self.root.expect("path of engine root is required"),
            disk_opts: self.disk_opts,
        }
    }
}
