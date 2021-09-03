use std::sync::Arc;

use async_trait::async_trait;
use futures::executor::block_on;
use tokio::sync::watch;

use super::background_job::BackgroundJob;
use super::manifest::Manifest;
use crate::error::Result;
use crate::file_system::FileSystem;
use crate::job::JobRuntime;
use crate::memtable::MemTable;
use crate::storage::*;

pub struct LocalStorage {
    manifest: Arc<Manifest>,
    current_rx: StorageVersionReceiver,
    background_job: BackgroundJob,
}

impl LocalStorage {
    pub fn new(
        options: StorageOptions,
        fs: Arc<dyn FileSystem>,
        job: Arc<dyn JobRuntime>,
    ) -> LocalStorage {
        let manifest = Arc::new(Manifest::new(options, fs));
        let current = block_on(manifest.current());
        let (tx, rx) = watch::channel(current);
        LocalStorage {
            manifest: manifest.clone(),
            current_rx: rx,
            background_job: BackgroundJob::new(manifest, job, tx),
        }
    }
}

#[async_trait]
impl Storage for LocalStorage {
    async fn current(&self) -> StorageVersionRef {
        self.manifest.current().await
    }

    fn current_rx(&self) -> StorageVersionReceiver {
        self.current_rx.clone()
    }

    async fn flush_memtable(&self, mem: Arc<dyn MemTable>) -> Result<StorageVersionRef> {
        let current = self.manifest.flush_memtable(mem).await?;
        self.background_job.schedule().await;
        Ok(current)
    }
}
