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

use std::path::PathBuf;

use rocksdb::DBCompressionType;
use serde::{Deserialize, Serialize};

use crate::{ExecutorConfig, NodeConfig, RaftConfig, RootConfig};

#[derive(Default, Clone, Debug, Deserialize, Serialize)]
pub struct Config {
    /// The root dir of engula server.
    pub root_dir: PathBuf,

    pub addr: String,

    pub cpu_nums: u32,

    pub init: bool,

    pub num_conn: usize,
    pub enable_proxy_service: bool,

    pub join_list: Vec<String>,

    #[serde(default)]
    pub node: NodeConfig,

    #[serde(default)]
    pub raft: RaftConfig,

    #[serde(default)]
    pub root: RootConfig,

    #[serde(default)]
    pub executor: ExecutorConfig,

    #[serde(default)]
    pub db: DbConfig,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DbConfig {
    // io related configs
    pub max_background_jobs: i32,
    pub max_sub_compactions: u32,
    pub max_manifest_file_size: usize,
    pub bytes_per_sync: u64,
    pub compaction_readahead_size: usize,
    pub use_direct_read: bool,
    pub use_direct_io_for_flush_and_compaction: bool,
    pub avoid_unnecessary_blocking_io: bool,

    // block & block cache cache related configs
    pub block_size: usize,
    pub block_cache_size: usize,

    // write buffer related configs
    pub write_buffer_size: usize,
    pub max_write_buffer_number: i32,
    pub min_write_buffer_number_to_merge: i32,

    pub num_levels: i32,
    pub compression_per_level: [DBCompressionType; 7],

    // compaction related configs
    pub level0_file_num_compaction_trigger: i32,
    pub target_file_size_base: u64,
    pub max_bytes_for_level_base: u64,
    pub max_bytes_for_level_multiplier: f64,
    pub max_compaction_bytes: u64,
    pub level_compaction_dynamic_level_bytes: bool,

    // write slowdown related configs
    pub level0_stop_write_trigger: i32,
    pub level0_slowdown_writes_trigger: i32,
    pub soft_pending_compaction_bytes_limit: usize,
    pub hard_pending_compaction_bytes_limit: usize,

    // rate limiter related configs
    pub rate_limiter_bytes_per_sec: i64,
    pub rate_limiter_refill_period: i64,
    pub rate_limiter_auto_tuned: bool,
}

impl Default for DbConfig {
    fn default() -> Self {
        DbConfig {
            max_background_jobs: 2,
            max_sub_compactions: 1,
            max_manifest_file_size: 1 << 30,
            bytes_per_sync: 1 << 20,
            compaction_readahead_size: 0,
            use_direct_read: false,
            use_direct_io_for_flush_and_compaction: false,
            avoid_unnecessary_blocking_io: true,

            block_size: 4 << 10,
            block_cache_size: adaptive_block_cache_size(),
            write_buffer_size: 64 << 20,
            max_write_buffer_number: 3,
            min_write_buffer_number_to_merge: 1,

            num_levels: 7,
            compression_per_level: [
                DBCompressionType::None,
                DBCompressionType::None,
                DBCompressionType::Lz4,
                DBCompressionType::Lz4,
                DBCompressionType::Lz4,
                DBCompressionType::Zstd,
                DBCompressionType::Zstd,
            ],

            level0_file_num_compaction_trigger: 4,
            target_file_size_base: 64 << 20,
            max_bytes_for_level_base: 256 << 20,
            max_bytes_for_level_multiplier: 10.0,
            max_compaction_bytes: 0,
            level_compaction_dynamic_level_bytes: true,

            level0_stop_write_trigger: 36,
            level0_slowdown_writes_trigger: 20,
            soft_pending_compaction_bytes_limit: 64 << 30,
            hard_pending_compaction_bytes_limit: 256 << 30,

            rate_limiter_bytes_per_sec: 10 << 30,
            rate_limiter_refill_period: 100_000,
            rate_limiter_auto_tuned: true,
        }
    }
}

fn adaptive_block_cache_size() -> usize {
    if cfg!(test) {
        return 32 << 20;
    }

    use sysinfo::{RefreshKind, System, SystemExt};
    let info = System::new_with_specifics(RefreshKind::new().with_memory());
    (info.total_memory() / 2) as usize
}
