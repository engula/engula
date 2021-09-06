use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::Mutex;
use tokio::task;
use tokio::time::{self, Duration};

use super::{Manifest, VersionDesc};
use crate::error::Result;
use crate::file_system::FileSystem;
use crate::format::{sst_name, FileDesc, SstOptions};
use crate::job::{CompactionInput, CompactionOutput, JobRuntime};
use crate::storage::*;

pub struct LocalManifest {
    job: Arc<dyn JobRuntime>,
    core: Arc<Core>,
    pending_job: Arc<Mutex<bool>>,
}

impl LocalManifest {
    pub fn new(
        options: StorageOptions,
        fs: Arc<dyn FileSystem>,
        job: Arc<dyn JobRuntime>,
    ) -> LocalManifest {
        LocalManifest {
            job,
            core: Arc::new(Core::new(options, fs)),
            pending_job: Arc::new(Mutex::new(false)),
        }
    }

    pub async fn schedule_background_jobs(&self) {
        let pending = self.pending_job.clone();
        {
            let mut pending = self.pending_job.lock().await;
            if *pending {
                return;
            }
            *pending = true;
        }

        let job = self.job.clone();
        let core = self.core.clone();
        let mut compaction = core.pick_compaction().await;
        if compaction.is_some() {
            task::spawn(async move {
                while let Some(input) = compaction {
                    let output = job.compact(input).await.unwrap();
                    core.install_compaction(output).await;
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
    async fn current(&self) -> Result<VersionDesc> {
        Ok(self.core.current().await)
    }

    async fn next_number(&self) -> Result<u64> {
        Ok(self.core.next_number())
    }

    async fn install_flush(&self, file: FileDesc) -> Result<VersionDesc> {
        let version = self.core.install_flush(file).await;
        self.schedule_background_jobs().await;
        Ok(version)
    }
}

pub struct Core {
    options: StorageOptions,
    next_number: AtomicU64,
    current: Mutex<VersionDesc>,
    obsoleted_files: Arc<Mutex<Vec<FileDesc>>>,
}

impl Core {
    fn new(options: StorageOptions, fs: Arc<dyn FileSystem>) -> Core {
        let obsoleted_files = Arc::new(Mutex::new(Vec::new()));
        let obsoleted_files_clone = obsoleted_files.clone();
        task::spawn(async move {
            purge_obsoleted_files(fs, obsoleted_files_clone).await;
        });
        Core {
            options,
            next_number: AtomicU64::new(0),
            current: Mutex::new(VersionDesc::default()),
            obsoleted_files,
        }
    }

    async fn current(&self) -> VersionDesc {
        self.current.lock().await.clone()
    }

    fn next_number(&self) -> u64 {
        self.next_number.fetch_add(1, Ordering::SeqCst)
    }

    async fn install_flush(&self, file: FileDesc) -> VersionDesc {
        let mut current = self.current.lock().await;
        current.files.push(file);
        current.clone()
    }

    async fn pick_compaction(&self) -> Option<CompactionInput> {
        let current = self.current.lock().await;
        if current.files.len() <= self.options.max_levels {
            return None;
        }
        let mut input_size = 0;
        let mut input_files = Vec::new();
        for file in current.files.iter() {
            if input_size < file.file_size / 2 && input_files.len() >= 2 {
                break;
            }
            input_size += file.file_size;
            input_files.push(file.clone());
        }
        let options = SstOptions {
            block_size: self.options.block_size,
        };
        Some(CompactionInput {
            options,
            input_files,
            output_file_number: self.next_number(),
        })
    }

    async fn install_compaction(&self, output: CompactionOutput) {
        let mut current = self.current.lock().await;
        let mut obsoleted_files = self.obsoleted_files.lock().await;
        let mut output_file = Some(output.output_file);
        for file in current.files.split_off(0) {
            if output
                .input_files
                .iter()
                .any(|x| x.file_number == file.file_number)
            {
                obsoleted_files.push(file);
                if let Some(output_file) = output_file.take() {
                    // Push the new level after the first obsoleted level.
                    current.files.push(output_file);
                }
            } else {
                current.files.push(file);
            }
        }
    }
}

async fn purge_obsoleted_files(
    fs: Arc<dyn FileSystem>,
    obsoleted_files: Arc<Mutex<Vec<FileDesc>>>,
) {
    let mut interval = time::interval(Duration::from_secs(3));
    loop {
        interval.tick().await;
        for file in obsoleted_files.lock().await.split_off(0) {
            let file_name = sst_name(file.file_number);
            fs.remove_file(&file_name).await.unwrap();
        }
    }
}
