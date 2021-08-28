use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::watch;

use super::storage::*;
use crate::common::Timestamp;
use crate::file_system::FileSystem;
use crate::memtable::MemTable;
use crate::Result;

pub struct LocalStorage {
    fs: Box<dyn FileSystem>,
    current: StorageVersionRef,
    current_tx: StorageVersionSender,
    current_rx: StorageVersionReceiver,
}

impl LocalStorage {
    pub fn new(fs: Box<dyn FileSystem>) -> Result<LocalStorage> {
        let current: StorageVersionRef = Arc::new(Box::new(LocalStorageVersion::new()));
        let (tx, rx) = watch::channel(current.clone());
        Ok(LocalStorage {
            fs,
            current,
            current_tx: tx,
            current_rx: rx,
        })
    }
}

#[async_trait]
impl Storage for LocalStorage {
    fn current(&self) -> StorageVersionRef {
        self.current.clone()
    }

    fn current_rx(&self) -> StorageVersionReceiver {
        self.current_rx.clone()
    }

    async fn flush_memtable(&self, mem: Arc<Box<dyn MemTable>>) -> Result<()> {
        Ok(())
    }
}

pub struct LocalStorageVersion {}

impl LocalStorageVersion {
    fn new() -> LocalStorageVersion {
        LocalStorageVersion {}
    }
}

#[async_trait]
impl StorageVersion for LocalStorageVersion {
    async fn get(&self, ts: Timestamp, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(None)
    }
}
