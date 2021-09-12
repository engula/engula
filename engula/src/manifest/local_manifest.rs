use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

use async_trait::async_trait;
use tokio::{sync::Mutex, task, time};

use super::{Manifest, ManifestOptions, VersionDesc};
use crate::{
    compaction::{CompactionInput, CompactionOutput, CompactionRuntime},
    error::Result,
    format::TableDesc,
    storage::Storage,
};

pub struct LocalManifest {
    core: Arc<Core>,
    runtime: Arc<dyn CompactionRuntime>,
    pending_compaction: Arc<Mutex<bool>>,
}

impl LocalManifest {
    pub fn new(
        options: ManifestOptions,
        storage: Arc<dyn Storage>,
        runtime: Arc<dyn CompactionRuntime>,
    ) -> LocalManifest {
        LocalManifest {
            core: Arc::new(Core::new(options, storage)),
            runtime,
            pending_compaction: Arc::new(Mutex::new(false)),
        }
    }

    pub async fn schedule_compaction(&self) {
        let pending = self.pending_compaction.clone();
        {
            let mut pending = self.pending_compaction.lock().await;
            if *pending {
                return;
            }
            *pending = true;
        }

        let core = self.core.clone();
        let runtime = self.runtime.clone();
        let mut compaction = core.pick_compaction().await;
        if compaction.is_some() {
            task::spawn(async move {
                while let Some(input) = compaction {
                    if let Ok(output) = runtime.compact(input).await {
                        core.install_compaction(output).await;
                    }
                    compaction = core.pick_compaction().await;
                }
                *pending.lock().await = false;
            });
        } else {
            *pending.lock().await = false;
        }
    }
}

#[async_trait]
impl Manifest for LocalManifest {
    async fn current(&self, _: u64) -> Result<VersionDesc> {
        Ok(self.core.current().await)
    }

    async fn add_table(&self, _: u64, table: TableDesc) -> Result<VersionDesc> {
        let version = self.core.add_table(table).await;
        self.schedule_compaction().await;
        Ok(version)
    }

    async fn next_number(&self) -> Result<u64> {
        Ok(self.core.next_number())
    }
}

pub struct Core {
    options: ManifestOptions,
    current: Mutex<VersionDesc>,
    next_number: AtomicU64,
    obsoleted_tables: Arc<Mutex<Vec<TableDesc>>>,
}

impl Core {
    fn new(options: ManifestOptions, storage: Arc<dyn Storage>) -> Core {
        let obsoleted_tables = Arc::new(Mutex::new(Vec::new()));
        let obsoleted_tables_clone = obsoleted_tables.clone();
        task::spawn(async move {
            purge_obsoleted_tables(storage, obsoleted_tables_clone).await;
        });
        Core {
            options,
            current: Mutex::new(VersionDesc::default()),
            next_number: AtomicU64::new(0),
            obsoleted_tables,
        }
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

    fn next_number(&self) -> u64 {
        self.next_number.fetch_add(1, Ordering::SeqCst)
    }

    async fn pick_compaction(&self) -> Option<CompactionInput> {
        let current = self.current.lock().await;
        if current.tables.len() <= self.options.num_levels {
            return None;
        }
        let mut input_size = 0;
        let mut input_tables = Vec::new();
        for table in current.tables.iter().rev() {
            if input_size < table.table_size / 2 && input_tables.len() >= 2 {
                break;
            }
            input_size += table.table_size;
            input_tables.push(table.clone());
        }
        Some(CompactionInput {
            tables: input_tables,
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
    let mut interval = time::interval(time::Duration::from_secs(3));
    loop {
        interval.tick().await;
        for table in obsoleted_tables.lock().await.split_off(0) {
            storage.remove_table(table.table_number).await.unwrap();
        }
    }
}
