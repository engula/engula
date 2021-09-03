mod local_storage;
mod manifest;

pub use local_storage::LocalStorage;

use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::watch;

use crate::error::Result;
use crate::format::Timestamp;
use crate::memtable::MemTable;

pub struct StorageOptions {
    pub max_levels: usize,
    pub block_size: usize,
}

impl StorageOptions {
    pub fn default() -> StorageOptions {
        StorageOptions {
            max_levels: 4,
            block_size: 8 * 1024,
        }
    }
}

pub type StorageVersionRef = Arc<dyn StorageVersion>;
#[allow(dead_code)]
pub type StorageVersionSender = watch::Sender<StorageVersionRef>;
pub type StorageVersionReceiver = watch::Receiver<StorageVersionRef>;

#[async_trait]
pub trait Storage: Send + Sync {
    async fn current(&self) -> StorageVersionRef;

    fn current_rx(&self) -> StorageVersionReceiver;

    async fn flush_memtable(&self, mem: Arc<dyn MemTable>) -> Result<StorageVersionRef>;
}

#[async_trait]
pub trait StorageVersion: Send + Sync {
    async fn get(&self, ts: Timestamp, key: &[u8]) -> Result<Option<Vec<u8>>>;
}
