use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use futures::executor::block_on;
use tokio::sync::{watch, Mutex};
use tokio::task;

use super::storage::*;
use crate::common::Timestamp;
use crate::error::Result;
use crate::file_system::FileSystem;
use crate::format::{SstBuilder, SstOptions, SstReader, TableBuilder, TableReader};
use crate::job::{CompactionInput, CompactionOutput, FileMeta, JobInput, JobOutput, JobRuntime};
use crate::memtable::MemTable;

pub struct LocalStorage {
    manifest: Arc<LocalManifest>,
    job: Arc<Box<dyn JobRuntime>>,
    pending_job: Arc<Mutex<bool>>,
    current_tx: Arc<StorageVersionSender>,
    current_rx: StorageVersionReceiver,
}

impl LocalStorage {
    pub fn new(
        options: StorageOptions,
        fs: Arc<Box<dyn FileSystem>>,
        job: Arc<Box<dyn JobRuntime>>,
    ) -> Result<LocalStorage> {
        let manifest = LocalManifest::new(options, fs);
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
                    if let Err(_) = current_tx.send(current) {
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

    async fn flush_memtable(&self, mem: Arc<Box<dyn MemTable>>) -> Result<StorageVersionRef> {
        let current = self.manifest.flush_memtable(mem).await?;
        self.schedule_background_jobs().await;
        Ok(current)
    }
}

struct LocalManifest {
    options: StorageOptions,
    fs: Arc<Box<dyn FileSystem>>,
    next_number: AtomicU64,
    version: Mutex<LocalVersion>,
    current: Mutex<StorageVersionRef>,
    obsoleted_levels: Mutex<Vec<Arc<Level>>>,
}

impl LocalManifest {
    fn new(options: StorageOptions, fs: Arc<Box<dyn FileSystem>>) -> LocalManifest {
        let version = LocalVersion::new();
        let current = version.to_ref();
        LocalManifest {
            options,
            fs,
            next_number: AtomicU64::new(0),
            version: Mutex::new(version),
            current: Mutex::new(current),
            obsoleted_levels: Mutex::new(Vec::new()),
        }
    }

    async fn current(&self) -> StorageVersionRef {
        self.current.lock().await.clone()
    }

    fn new_file_name(&self) -> String {
        let file_number = self.next_number.fetch_add(1, Ordering::SeqCst);
        format!("{}.sst", file_number)
    }

    async fn open_sst_reader(&self, meta: &FileMeta) -> Result<SstReader> {
        let file = self.fs.new_random_access_reader(&meta.file_name).await?;
        SstReader::open(file, meta.file_size).await
    }

    async fn flush_memtable(&self, mem: Arc<Box<dyn MemTable>>) -> Result<StorageVersionRef> {
        let options = SstOptions::default();
        let file_name = self.new_file_name();
        let wfile = self.fs.new_sequential_writer(&file_name).await?;
        let mut builder = SstBuilder::new(options, wfile);
        let snap = mem.snapshot().await;
        let mut iter = snap.iter();
        for v in iter.next() {
            builder.add(v.0, v.1, v.2).await;
        }
        let file_size = builder.finish().await?;
        let file_meta = FileMeta {
            file_name,
            file_size,
        };
        let current = self.install_flush_output(file_meta).await?;
        Ok(current)
    }

    async fn install_flush_output(&self, meta: FileMeta) -> Result<StorageVersionRef> {
        let reader = self.open_sst_reader(&meta).await?;
        let level = Level { meta, reader };
        let new_current = {
            let mut version = self.version.lock().await;
            version.levels.push_front(Arc::new(level));
            let mut current = self.current.lock().await;
            *current = version.to_ref();
            current.clone()
        };
        self.purge_obsoleted_levels().await?;
        Ok(new_current)
    }

    async fn pick_compaction(&self) -> Option<CompactionInput> {
        let version = self.version.lock().await;
        if version.levels.len() <= self.options.max_levels {
            return None;
        }
        let mut input_size = 0;
        let mut input_levels = Vec::new();
        for level in version.levels.iter() {
            if input_size < level.meta.file_size / 2 && input_levels.len() >= 2 {
                break;
            }
            input_size += level.meta.file_size;
            input_levels.push(level.meta.clone());
        }
        Some(CompactionInput {
            levels: input_levels,
            output_file_name: self.new_file_name(),
        })
    }

    async fn install_compaction_output(&self, c: CompactionOutput) -> Result<StorageVersionRef> {
        let reader = self.open_sst_reader(&c.output).await?;
        let mut new_level = Some(Level {
            meta: c.output,
            reader,
        });

        let new_current = {
            let mut version = self.version.lock().await;
            let mut obsoleted_levels = self.obsoleted_levels.lock().await;
            for level in version.levels.split_off(0) {
                if c.input.iter().any(|x| x.file_name == level.meta.file_name) {
                    obsoleted_levels.push(level);
                    if let Some(new_level) = new_level.take() {
                        // Push the new level after the first obsoleted level.
                        version.levels.push_back(Arc::new(new_level));
                    }
                } else {
                    version.levels.push_back(level);
                }
            }
            let mut current = self.current.lock().await;
            *current = version.to_ref();
            current.clone()
        };

        self.purge_obsoleted_levels().await?;
        Ok(new_current)
    }

    async fn purge_obsoleted_levels(&self) -> Result<()> {
        let mut levels = self.obsoleted_levels.lock().await;
        for level in levels.split_off(0) {
            match Arc::try_unwrap(level) {
                Ok(level) => self.fs.remove_file(&level.meta.file_name).await?,
                Err(level) => levels.push(level),
            }
        }
        Ok(())
    }
}

struct Level {
    meta: FileMeta,
    reader: SstReader,
}

struct LocalVersion {
    levels: VecDeque<Arc<Level>>,
}

impl LocalVersion {
    fn new() -> LocalVersion {
        LocalVersion {
            levels: VecDeque::new(),
        }
    }

    fn to_ref(&self) -> StorageVersionRef {
        Arc::new(Box::new(LocalVersion {
            levels: self.levels.clone(),
        }))
    }
}

#[async_trait]
impl StorageVersion for LocalVersion {
    async fn get(&self, ts: Timestamp, key: &[u8]) -> Result<Option<Vec<u8>>> {
        for level in self.levels.iter() {
            if let Some(v) = level.reader.get(ts, key).await? {
                return Ok(Some(v));
            }
        }
        Ok(None)
    }
}
