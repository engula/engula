use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc,
};

use async_trait::async_trait;
use metrics::{counter, histogram};
use tokio::{
    sync::Mutex,
    task,
    time::{self, Duration, Instant},
};
use tracing::{error, info};

use super::{Manifest, ManifestOptions, VersionDesc};
use crate::{
    compaction::{CompactionInput, CompactionOutput, CompactionRuntime},
    error::{Error, Result},
    format::TableDesc,
    storage::Storage,
};

pub struct LocalManifest {
    cores: Vec<Arc<Core>>,
    next_number: Arc<AtomicU64>,
}

impl LocalManifest {
    pub fn new(
        options: ManifestOptions,
        storage: Arc<dyn Storage>,
        runtime: Arc<dyn CompactionRuntime>,
    ) -> LocalManifest {
        let next_number = Arc::new(AtomicU64::new(0));
        let obsoleted_tables = Arc::new(Mutex::new(Vec::new()));

        let mut cores = Vec::new();
        for id in 0..options.num_shards {
            let core = Arc::new(Core::new(
                format!("shard:{}", id),
                options.clone(),
                runtime.clone(),
                next_number.clone(),
                obsoleted_tables.clone(),
            ));
            cores.push(core);
        }

        task::spawn(async move {
            purge_obsoleted_tables(storage, obsoleted_tables).await;
        });

        LocalManifest { cores, next_number }
    }
}

#[async_trait]
impl Manifest for LocalManifest {
    async fn current(&self, id: u64) -> Result<VersionDesc> {
        if let Some(core) = self.cores.get(id as usize) {
            Ok(core.current().await)
        } else {
            Err(Error::NotFound(format!("no such shard {}", id)))
        }
    }

    async fn add_table(&self, id: u64, table: TableDesc) -> Result<VersionDesc> {
        if let Some(core) = self.cores.get(id as usize) {
            let version = core.add_table(table).await;
            if core.need_compact().await {
                let core = core.clone();
                task::spawn(async move {
                    core.run_compact().await;
                });
            }
            Ok(version)
        } else {
            Err(Error::NotFound(format!("no such shard {}", id)))
        }
    }

    async fn next_number(&self) -> Result<u64> {
        Ok(self.next_number.fetch_add(1, Ordering::SeqCst))
    }
}

pub struct Core {
    name: String,
    options: ManifestOptions,
    current: Mutex<VersionDesc>,
    runtime: Arc<dyn CompactionRuntime>,
    pending_compaction: AtomicBool,
    next_number: Arc<AtomicU64>,
    obsoleted_tables: Arc<Mutex<Vec<TableDesc>>>,
}

impl Core {
    fn new(
        name: String,
        options: ManifestOptions,
        runtime: Arc<dyn CompactionRuntime>,
        next_number: Arc<AtomicU64>,
        obsoleted_tables: Arc<Mutex<Vec<TableDesc>>>,
    ) -> Core {
        Core {
            name,
            options,
            current: Mutex::new(VersionDesc::default()),
            runtime,
            pending_compaction: AtomicBool::new(false),
            next_number,
            obsoleted_tables,
        }
    }

    fn next_number(&self) -> u64 {
        self.next_number.fetch_add(1, Ordering::SeqCst)
    }

    async fn current(&self) -> VersionDesc {
        self.current.lock().await.clone()
    }

    async fn add_table(&self, table: TableDesc) -> VersionDesc {
        let mut current = self.current.lock().await;
        current.tables.push(table);
        current.sequence += 1;
        current.clone()
    }

    async fn run_compact(&self) {
        if self
            .pending_compaction
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            return;
        }
        while let Some(input) = self.pick_compaction().await {
            info!("[{}] start compaction {:?}", self.name, input);
            let start = Instant::now();
            let input_size = input.input_size;
            match self.runtime.compact(input).await {
                Ok(output) => {
                    let throughput = input_size as f64 / start.elapsed().as_secs_f64();
                    counter!("engula.compact.bytes", input_size as u64);
                    histogram!("engula.compact.throughput", throughput);
                    info!(
                        "[{}] finish compaction {:?} throughput {} MB/s",
                        self.name,
                        output,
                        throughput as u64 / 1024 / 1024
                    );
                    self.install_compaction(output).await;
                }
                Err(err) => error!("[{}] compaction failed: {}", self.name, err),
            }
        }
    }

    async fn need_compact(&self) -> bool {
        if self.pending_compaction.load(Ordering::Acquire) {
            return false;
        }
        let current = self.current.lock().await;
        current.tables.len() > self.options.num_levels
    }

    async fn pick_compaction(&self) -> Option<CompactionInput> {
        let current = self.current.lock().await;
        if current.tables.len() <= self.options.num_levels {
            return None;
        }
        let mut input_size = 0;
        let mut input_tables = Vec::new();
        for table in current.tables.iter().rev() {
            let table_size = table.sst_table_size + table.parquet_table_size;
            if input_size < table_size / 2 && input_tables.len() >= 2 {
                break;
            }
            input_size += table_size;
            input_tables.push(table.clone());
        }
        Some(CompactionInput {
            tables: input_tables,
            input_size,
            output_table_number: self.next_number(),
        })
    }

    async fn install_compaction(&self, compaction: CompactionOutput) {
        let mut current = self.current.lock().await;
        let mut output_table = compaction.output_table;
        let mut obsoleted_tables = self.obsoleted_tables.lock().await;
        for table in current.tables.split_off(0) {
            if compaction
                .tables
                .iter()
                .any(|x| x.table_number == table.table_number)
            {
                obsoleted_tables.push(table);
                // Replaces the first obsoleted table with the output table.
                if let Some(output_table) = output_table.take() {
                    current.tables.push(output_table);
                }
            } else {
                current.tables.push(table);
            }
        }
        current.sequence += 1;
    }
}

async fn purge_obsoleted_tables(
    storage: Arc<dyn Storage>,
    obsoleted_tables: Arc<Mutex<Vec<TableDesc>>>,
) {
    // This is ugly and unsafe, but good enough for the demo.
    let mut interval = time::interval(Duration::from_secs(3));
    loop {
        interval.tick().await;
        for table in obsoleted_tables.lock().await.split_off(0) {
            if let Err(err) = storage.remove_table(table.table_number).await {
                error!("remove table {}: {}", table.table_number, err);
            }
        }
    }
}
