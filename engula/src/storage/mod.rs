mod local_storage;

pub use local_storage::LocalStorage;

use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::watch;

use crate::common::Timestamp;
use crate::error::Result;
use crate::memtable::MemTable;

pub type StorageVersionRef = Arc<Box<dyn StorageVersion>>;
#[allow(dead_code)]
pub type StorageVersionSender = watch::Sender<StorageVersionRef>;
pub type StorageVersionReceiver = watch::Receiver<StorageVersionRef>;

#[async_trait]
pub trait Storage: Send + Sync {
    async fn current(&self) -> StorageVersionRef;

    fn current_rx(&self) -> StorageVersionReceiver;

    async fn flush_memtable(&self, mem: Arc<Box<dyn MemTable>>) -> Result<StorageVersionRef>;
}

#[async_trait]
pub trait StorageVersion: Send + Sync {
    async fn get(&self, ts: Timestamp, key: &[u8]) -> Result<Option<Vec<u8>>>;
}

pub struct StorageOptions {
    pub max_levels: usize,
}

impl StorageOptions {
    pub fn default() -> StorageOptions {
        StorageOptions { max_levels: 4 }
    }
}
