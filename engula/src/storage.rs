use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::watch;

use crate::entry::Timestamp;
use crate::Result;

pub type StorageVersionRef = Arc<Box<dyn StorageVersion>>;
pub type StorageVersionSender = watch::Sender<Arc<Box<dyn StorageVersion>>>;
pub type StorageVersionReceiver = watch::Receiver<Arc<Box<dyn StorageVersion>>>;

pub trait Storage: Send + Sync {
    fn current(&self) -> StorageVersionRef;

    fn current_receiver(&self) -> StorageVersionReceiver;
}

#[async_trait]
pub trait StorageVersion: Send + Sync {
    async fn get(&self, ts: Timestamp, key: &[u8]) -> Result<Option<Vec<u8>>>;
}

pub struct MemStorage {
    current_tx: StorageVersionSender,
    current_rx: StorageVersionReceiver,
}

impl MemStorage {
    pub fn new() -> MemStorage {
        let current: Box<dyn StorageVersion> = Box::new(MemStorageVersion::new());
        let current = Arc::new(current);
        let (tx, rx) = watch::channel(current);
        MemStorage {
            current_tx: tx,
            current_rx: rx,
        }
    }
}

impl Storage for MemStorage {
    fn current(&self) -> StorageVersionRef {
        Arc::new(Box::new(MemStorageVersion::new()))
    }

    fn current_receiver(&self) -> StorageVersionReceiver {
        self.current_rx.clone()
    }
}

pub struct MemStorageVersion {}

impl MemStorageVersion {
    fn new() -> MemStorageVersion {
        MemStorageVersion {}
    }
}

#[async_trait]
impl StorageVersion for MemStorageVersion {
    async fn get(&self, ts: Timestamp, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(None)
    }
}
