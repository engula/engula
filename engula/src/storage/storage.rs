use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::watch;

use crate::common::Timestamp;
use crate::Result;

pub type StorageVersionRef = Arc<Box<dyn StorageVersion>>;
pub type StorageVersionSender = watch::Sender<StorageVersionRef>;
pub type StorageVersionReceiver = watch::Receiver<StorageVersionRef>;

pub trait Storage: Send + Sync {
    fn current(&self) -> StorageVersionRef;

    fn current_rx(&self) -> StorageVersionReceiver;
}

#[async_trait]
pub trait StorageVersion: Send + Sync {
    async fn get(&self, ts: Timestamp, key: &[u8]) -> Result<Option<Vec<u8>>>;
}
