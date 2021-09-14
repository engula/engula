use std::sync::Arc;

use tokio::sync::Mutex;

use crate::{
    error::Result,
    format::{TableDesc, TableReader, Timestamp},
    manifest::{Manifest, VersionDesc},
    memtable::MemTable,
    storage::Storage,
};

struct Table {
    desc: TableDesc,
    reader: Box<dyn TableReader>,
}

pub struct Version {
    tables: Vec<Arc<Table>>,
    sequence: u64,
}

impl Version {
    fn new() -> Version {
        Version {
            tables: Vec::new(),
            sequence: 0,
        }
    }

    pub async fn get(&self, ts: Timestamp, key: &[u8]) -> Result<Option<Vec<u8>>> {
        // The last table contains the latest data.
        for table in self.tables.iter().rev() {
            if let Some(v) = table.reader.get(ts, key).await? {
                return Ok(Some(v));
            }
        }
        Ok(None)
    }

    pub fn sequence(&self) -> u64 {
        self.sequence
    }
}

pub struct VersionSet {
    id: u64,
    current: Mutex<Arc<Version>>,
    storage: Arc<dyn Storage>,
    manifest: Arc<dyn Manifest>,
}

impl VersionSet {
    pub fn new(id: u64, storage: Arc<dyn Storage>, manifest: Arc<dyn Manifest>) -> VersionSet {
        VersionSet {
            id,
            current: Mutex::new(Arc::new(Version::new())),
            storage,
            manifest,
        }
    }

    pub async fn current(&self) -> Result<Arc<Version>> {
        let version = self.manifest.current(self.id).await?;
        self.install_version(version).await
    }

    pub async fn flush_memtable(&self, mem: Arc<dyn MemTable>) -> Result<Arc<Version>> {
        let number = self.manifest.next_number().await?;
        let mut builder = self.storage.new_builder(number).await?;
        let snapshot = mem.snapshot().await;
        for ent in snapshot.iter() {
            builder.add(ent.0, ent.1, ent.2).await;
        }
        let table = builder.finish().await?;
        let version = self.manifest.add_table(self.id, table).await?;
        self.install_version(version).await
    }

    async fn install_version(&self, version: VersionDesc) -> Result<Arc<Version>> {
        let mut current = self.current.lock().await;
        if current.sequence() >= version.sequence {
            return Ok(current.clone());
        }
        let mut tables = Vec::new();
        for desc in version.tables {
            if let Some(table) = current
                .tables
                .iter()
                .find(|x| x.desc.table_number == desc.table_number)
            {
                tables.push(table.clone());
            } else {
                let reader = self.storage.new_reader(desc.clone()).await?;
                let table = Arc::new(Table { desc, reader });
                tables.push(table);
            }
        }
        *current = Arc::new(Version {
            tables,
            sequence: version.sequence,
        });
        Ok(current.clone())
    }
}
