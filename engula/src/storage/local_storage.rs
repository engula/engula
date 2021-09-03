use std::sync::Arc;

use async_trait::async_trait;
use futures::executor::block_on;
use tokio::sync::{watch, Mutex};
use tokio::task;

use super::manifest::Manifest;
use crate::error::Result;
use crate::file_system::FileSystem;
use crate::job::{JobInput, JobOutput, JobRuntime};
use crate::memtable::MemTable;
use crate::storage::*;

pub struct LocalStorage {
    manifest: Arc<Manifest>,
    job: Arc<dyn JobRuntime>,
    pending_job: Arc<Mutex<bool>>,
    current_tx: Arc<StorageVersionSender>,
    current_rx: StorageVersionReceiver,
}

impl LocalStorage {
    pub fn new(
        options: StorageOptions,
        fs: Arc<dyn FileSystem>,
        job: Arc<dyn JobRuntime>,
    ) -> Result<LocalStorage> {
        let manifest = Manifest::new(options, fs);
        let current = block_on(manifest.current());
        let (tx, rx) = watch::channel(current);
        Ok(LocalStorage {
            manifest: Arc::new(manifest),
            job,
            pending_job: Arc::new(Mutex::new(false)),
            current_tx: Arc::new(tx),
            current_rx: rx,
        })
    }

    async fn schedule_background_jobs(&self) {
        let pending_job = self.pending_job.clone();
        {
            let mut pending_job = self.pending_job.lock().await;
            if *pending_job {
                return;
            }
            *pending_job = true;
        }

        let job = self.job.clone();
        let manifest = self.manifest.clone();
        let current_tx = self.current_tx.clone();
        let mut compaction = manifest.pick_compaction().await;
        if compaction.is_some() {
            task::spawn(async move {
                while let Some(input) = compaction {
                    let input = JobInput::Compaction(input);
                    let output = job.spawn(input).await.unwrap();
                    let JobOutput::Compaction(output) = output;
                    let current = manifest.install_compaction_output(output).await.unwrap();
                    if current_tx.send(current).is_err() {
                        panic!("send current error");
                    }
                    compaction = manifest.pick_compaction().await;
                }
                *pending_job.lock().await = false;
            });
        } else {
            *pending_job.lock().await = false;
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
        self.schedule_background_jobs().await;
        Ok(current)
    }
}
