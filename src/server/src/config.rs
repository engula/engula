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

use std::{path::PathBuf, time::Duration};

use rocksdb::DBCompressionType;
use serde::{Deserialize, Serialize};

use crate::constants::REPLICA_PER_GROUP;

#[derive(Default, Clone, Debug, Deserialize, Serialize)]
pub struct Config {
    /// The root dir of engula server.
    pub root_dir: PathBuf,

    pub addr: String,

    pub cpu_nums: u32,

    pub init: bool,

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

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct NodeConfig {
    /// The limit bytes of each shard chunk during migration.
    ///
    /// Default: 64KB.
    pub shard_chunk_size: usize,

    /// The limit number of keys for gc shard after migration.
    ///
    /// Default: 256.
    pub shard_gc_keys: usize,

    #[serde(default)]
    pub replica: ReplicaConfig,

    #[serde(default)]
    pub engine: EngineConfig,
}

#[derive(Clone, Debug, Default)]
pub struct ReplicaTestingKnobs {
    pub disable_scheduler_orphan_replica_detecting_intervals: bool,
    pub disable_scheduler_durable_task: bool,
    pub disable_scheduler_remove_orphan_replica_task: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ReplicaConfig {
    /// The limit size of each snapshot files.
    ///
    /// Default: 64MB.
    pub snap_file_size: u64,

    #[serde(skip)]
    pub testing_knobs: ReplicaTestingKnobs,
}

#[derive(Clone, Default, Debug, Serialize, Deserialize)]
pub struct EngineConfig {
    /// Log slow io requests if it exceeds the specified threshold.
    ///
    /// Default: disabled
    pub engine_slow_io_threshold_ms: Option<u64>,
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

#[derive(Clone, Debug, Default)]
pub struct RaftTestingKnobs {
    pub force_new_peer_receiving_snapshot: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RaftConfig {
    /// The intervals of tick, in millis.
    ///
    /// Default: 500ms.
    pub tick_interval_ms: u64,

    /// The size of inflights requests.
    ///
    /// Default: 102400
    pub max_inflight_requests: usize,

    /// Before a follower begin election, it must wait a randomly election ticks and does not
    /// receives any messages from leader.
    ///
    /// Default: 3.
    pub election_tick: usize,

    /// Limit the entries batched in an append message(in size). 0 means one entry per message.
    ///
    /// Default: 64KB
    pub max_size_per_msg: u64,

    /// Limit the total bytes per io batch requests.
    ///
    /// Default: 64KB
    pub max_io_batch_size: u64,

    /// Limit the number of inflights messages which send to one peer.
    ///
    /// Default: 10K
    pub max_inflight_msgs: usize,

    /// Log slow io requests if it exceeds the specified threshold.
    ///
    /// Default: disabled
    pub engine_slow_io_threshold_ms: Option<u64>,

    /// Enable recycle log files to reduce allocating overhead?
    ///
    /// Default: false
    pub enable_log_recycle: bool,

    #[serde(skip)]
    pub testing_knobs: RaftTestingKnobs,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RootConfig {
    pub replicas_per_group: usize,
    pub enable_group_balance: bool,
    pub enable_replica_balance: bool,
    pub enable_shard_balance: bool,
    pub enable_leader_balance: bool,
    pub liveness_threshold_sec: u64,
    pub heartbeat_timeout_sec: u64,
    pub schedule_interval_sec: u64,
    pub max_create_group_retry_before_rollback: u64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ExecutorConfig {
    pub event_interval: Option<u32>,
    pub global_event_interval: Option<u32>,
    pub max_blocking_threads: Option<usize>,
}

impl Default for NodeConfig {
    fn default() -> Self {
        NodeConfig {
            shard_chunk_size: 64 * 1024 * 1024,
            shard_gc_keys: 256,
            replica: ReplicaConfig::default(),
            engine: EngineConfig::default(),
        }
    }
}

impl Default for ReplicaConfig {
    fn default() -> Self {
        ReplicaConfig {
            snap_file_size: 64 * 1024 * 1024 * 1024,
            testing_knobs: ReplicaTestingKnobs::default(),
        }
    }
}

impl DbConfig {
    pub fn to_options(&self) -> rocksdb::Options {
        use rocksdb::{BlockBasedIndexType, BlockBasedOptions, Cache, Options};

        let cfg = self;

        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);

        opts.set_max_background_jobs(cfg.max_background_jobs);
        opts.set_max_subcompactions(cfg.max_sub_compactions);
        opts.set_max_manifest_file_size(cfg.max_manifest_file_size);
        opts.set_bytes_per_sync(cfg.bytes_per_sync);
        opts.set_compaction_readahead_size(cfg.compaction_readahead_size);
        opts.set_use_direct_reads(cfg.use_direct_read);
        opts.set_use_direct_io_for_flush_and_compaction(cfg.use_direct_io_for_flush_and_compaction);
        opts.set_avoid_unnecessary_blocking_io(cfg.avoid_unnecessary_blocking_io);

        opts.set_write_buffer_size(cfg.write_buffer_size);
        opts.set_max_write_buffer_number(cfg.max_write_buffer_number);
        opts.set_min_write_buffer_number_to_merge(cfg.min_write_buffer_number_to_merge);

        opts.set_num_levels(cfg.num_levels);
        opts.set_compression_per_level(&cfg.compression_per_level);

        opts.set_level_zero_file_num_compaction_trigger(cfg.level0_file_num_compaction_trigger);
        opts.set_target_file_size_base(cfg.target_file_size_base);
        opts.set_max_bytes_for_level_base(cfg.max_bytes_for_level_base);
        opts.set_max_bytes_for_level_multiplier(cfg.max_bytes_for_level_multiplier);
        opts.set_max_compaction_bytes(cfg.max_compaction_bytes);
        opts.set_level_compaction_dynamic_level_bytes(true);

        opts.set_level_zero_slowdown_writes_trigger(cfg.level0_slowdown_writes_trigger);
        opts.set_level_zero_stop_writes_trigger(cfg.level0_slowdown_writes_trigger);
        opts.set_soft_pending_compaction_bytes_limit(cfg.soft_pending_compaction_bytes_limit);
        opts.set_hard_pending_compaction_bytes_limit(cfg.hard_pending_compaction_bytes_limit);

        opts.set_auto_tuned_ratelimiter(
            cfg.rate_limiter_bytes_per_sec,
            cfg.rate_limiter_refill_period,
            10,
            cfg.rate_limiter_auto_tuned,
        );

        let cache = Cache::new_lru_cache(cfg.block_cache_size).expect("new lrc cache");

        let mut blk_opts = BlockBasedOptions::default();
        blk_opts.set_index_type(BlockBasedIndexType::TwoLevelIndexSearch);
        blk_opts.set_block_size(cfg.block_size);
        blk_opts.set_block_cache(&cache);
        blk_opts.set_cache_index_and_filter_blocks(true);
        blk_opts.set_bloom_filter(10.0, false);
        opts.set_block_based_table_factory(&blk_opts);

        opts.create_missing_column_families(true);
        opts
    }
}

impl Default for DbConfig {
    fn default() -> Self {
        DbConfig {
            max_background_jobs: adaptive_max_background_jobs(),
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
            max_write_buffer_number: 5,
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

impl RaftConfig {
    pub(crate) fn to_raft_config(&self, replica_id: u64, applied: u64) -> raft::Config {
        raft::Config {
            id: replica_id,
            election_tick: self.election_tick,
            heartbeat_tick: 1,
            applied,
            pre_vote: true,
            batch_append: true,
            check_quorum: true,
            max_size_per_msg: self.max_size_per_msg,
            max_inflight_msgs: self.max_inflight_msgs,
            max_committed_size_per_ready: self.max_io_batch_size,
            read_only_option: raft::ReadOnlyOption::Safe,
            ..Default::default()
        }
    }
}

impl Default for RaftConfig {
    fn default() -> Self {
        RaftConfig {
            tick_interval_ms: 500,
            max_inflight_requests: 102400,
            election_tick: 3,
            max_size_per_msg: 64 << 10,
            max_io_batch_size: 64 << 10,
            max_inflight_msgs: 10 * 1000,
            engine_slow_io_threshold_ms: None,
            enable_log_recycle: false,
            testing_knobs: RaftTestingKnobs::default(),
        }
    }
}

impl RootConfig {
    pub fn heartbeat_interval(&self) -> Duration {
        Duration::from_secs(self.liveness_threshold_sec - self.heartbeat_timeout_sec)
    }
}

impl Default for RootConfig {
    fn default() -> Self {
        Self {
            replicas_per_group: REPLICA_PER_GROUP,
            enable_group_balance: true,
            enable_replica_balance: true,
            enable_shard_balance: true,
            enable_leader_balance: true,
            liveness_threshold_sec: 30,
            heartbeat_timeout_sec: 4,
            schedule_interval_sec: 3,
            max_create_group_retry_before_rollback: 10,
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

fn adaptive_max_background_jobs() -> i32 {
    use std::cmp::{max, min};

    #[allow(clippy::manual_clamp)]
    max(min(num_cpus::get() as i32, 8), 2)
}
