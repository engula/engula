use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::Mutex;

use crate::error::Result;
use crate::file_system::FileSystem;
use crate::format::{
    sst_name, FileDesc, SstBuilder, SstOptions, SstReader, TableBuilder, TableReader, Timestamp,
};
use crate::job::{CompactionInput, CompactionOutput};
use crate::memtable::MemTable;
use crate::storage::*;

struct File {
    desc: FileDesc,
    reader: SstReader,
}

struct ManifestVersion {
    files: VecDeque<Arc<File>>,
}

impl ManifestVersion {
    fn new() -> ManifestVersion {
        ManifestVersion {
            files: VecDeque::new(),
        }
    }

    fn to_ref(&self) -> StorageVersionRef {
        Arc::new(ManifestVersion {
            files: self.files.clone(),
        })
    }
}

#[async_trait]
impl StorageVersion for ManifestVersion {
    async fn get(&self, ts: Timestamp, key: &[u8]) -> Result<Option<Vec<u8>>> {
        for file in self.files.iter() {
            if let Some(v) = file.reader.get(ts, key).await? {
                return Ok(Some(v));
            }
        }
        Ok(None)
    }
}

pub struct Manifest {
    options: StorageOptions,
    fs: Arc<dyn FileSystem>,
    next_number: AtomicU64,
    version: Mutex<ManifestVersion>,
    current: Mutex<StorageVersionRef>,
    obsoleted_files: Mutex<Vec<Arc<File>>>,
}

impl Manifest {
    pub fn new(options: StorageOptions, fs: Arc<dyn FileSystem>) -> Manifest {
        let version = ManifestVersion::new();
        let current = version.to_ref();
        Manifest {
            options,
            fs,
            next_number: AtomicU64::new(0),
            version: Mutex::new(version),
            current: Mutex::new(current),
            obsoleted_files: Mutex::new(Vec::new()),
        }
    }

    pub async fn current(&self) -> StorageVersionRef {
        self.current.lock().await.clone()
    }

    fn new_file_number(&self) -> u64 {
        self.next_number.fetch_add(1, Ordering::SeqCst)
    }

    async fn open_sst_reader(&self, desc: &FileDesc) -> Result<SstReader> {
        let file_name = sst_name(desc.file_number);
        let file_reader = self.fs.new_random_access_reader(&file_name).await?;
        SstReader::open(file_reader, desc.file_size).await
    }

    pub async fn flush_memtable(&self, mem: Arc<dyn MemTable>) -> Result<StorageVersionRef> {
        let options = SstOptions::default();
        let file_number = self.new_file_number();
        let file_name = sst_name(file_number);
        let writer = self.fs.new_sequential_writer(&file_name).await?;
        let mut builder = SstBuilder::new(options, writer);
        let snap = mem.snapshot().await;
        for v in snap.iter() {
            builder.add(v.0, v.1, v.2).await
        }
        let file_size = builder.finish().await?;
        let file_desc = FileDesc {
            file_number,
            file_size: file_size as u64,
        };
        let current = self.install_flush_output(file_desc).await?;
        Ok(current)
    }

    async fn install_flush_output(&self, desc: FileDesc) -> Result<StorageVersionRef> {
        let reader = self.open_sst_reader(&desc).await?;
        let file = File { desc, reader };
        let new_current = {
            let mut version = self.version.lock().await;
            version.files.push_front(Arc::new(file));
            let mut current = self.current.lock().await;
            *current = version.to_ref();
            current.clone()
        };
        self.purge_obsoleted_files().await?;
        Ok(new_current)
    }

    pub async fn pick_compaction(&self) -> Option<CompactionInput> {
        let version = self.version.lock().await;
        if version.files.len() <= self.options.max_levels {
            return None;
        }
        let mut input_size = 0;
        let mut input_files = Vec::new();
        for file in version.files.iter() {
            if input_size < file.desc.file_size / 2 && input_files.len() >= 2 {
                break;
            }
            input_size += file.desc.file_size;
            input_files.push(file.desc.clone());
        }
        let options = SstOptions {
            block_size: self.options.block_size,
        };
        Some(CompactionInput {
            options,
            input_files,
            output_file_number: self.new_file_number(),
        })
    }

    pub async fn install_compaction_output(
        &self,
        c: CompactionOutput,
    ) -> Result<StorageVersionRef> {
        let reader = self.open_sst_reader(&c.output_file).await?;
        let mut output_file = Some(File {
            desc: c.output_file,
            reader,
        });

        let new_current = {
            let mut version = self.version.lock().await;
            let mut obsoleted_files = self.obsoleted_files.lock().await;
            for file in version.files.split_off(0) {
                if c.input_files
                    .iter()
                    .any(|x| x.file_number == file.desc.file_number)
                {
                    obsoleted_files.push(file);
                    if let Some(output_file) = output_file.take() {
                        // Push the new level after the first obsoleted level.
                        version.files.push_back(Arc::new(output_file));
                    }
                } else {
                    version.files.push_back(file);
                }
            }
            let mut current = self.current.lock().await;
            *current = version.to_ref();
            current.clone()
        };

        self.purge_obsoleted_files().await?;
        Ok(new_current)
    }

    async fn purge_obsoleted_files(&self) -> Result<()> {
        let mut files = self.obsoleted_files.lock().await;
        for file in files.split_off(0) {
            match Arc::try_unwrap(file) {
                Ok(file) => {
                    let file_name = sst_name(file.desc.file_number);
                    self.fs.remove_file(&file_name).await?;
                }
                Err(file) => files.push(file),
            }
        }
        Ok(())
    }
}
