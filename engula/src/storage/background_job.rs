use std::sync::Arc;

use tokio::sync::Mutex;
use tokio::task;

use super::manifest::Manifest;
use crate::job::{JobInput, JobOutput, JobRuntime};
use crate::storage::StorageVersionSender;

pub struct BackgroundJob {
    manifest: Arc<Manifest>,
    runtime: Arc<dyn JobRuntime>,
    pending: Arc<Mutex<bool>>,
    current_tx: Arc<StorageVersionSender>,
}

impl BackgroundJob {
    pub fn new(
        manifest: Arc<Manifest>,
        runtime: Arc<dyn JobRuntime>,
        current_tx: StorageVersionSender,
    ) -> BackgroundJob {
        BackgroundJob {
            manifest,
            runtime,
            pending: Arc::new(Mutex::new(false)),
            current_tx: Arc::new(current_tx),
        }
    }

    pub async fn schedule(&self) {
        let pending = self.pending.clone();
        {
            let mut pending = self.pending.lock().await;
            if *pending {
                return;
            }
            *pending = true;
        }

        let runtime = self.runtime.clone();
        let manifest = self.manifest.clone();
        let current_tx = self.current_tx.clone();
        let mut compaction = manifest.pick_compaction().await;
        if compaction.is_some() {
            task::spawn(async move {
                while let Some(input) = compaction {
                    let input = JobInput::Compaction(input);
                    let output = runtime.spawn(input).await.unwrap();
                    let JobOutput::Compaction(output) = output;
                    let current = manifest.install_compaction_output(output).await.unwrap();
                    if current_tx.send(current).is_err() {
                        panic!("send current error");
                    }
                    compaction = manifest.pick_compaction().await;
                }
                *pending.lock().await = false;
            });
        } else {
            *pending.lock().await = false;
        }
    }
}
