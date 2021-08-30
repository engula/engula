use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::{watch, Mutex};

use super::storage::*;
use crate::common::Timestamp;
use crate::error::Result;
use crate::file_system::FileSystem;
use crate::format::{SstBuilder, SstOptions, SstReader, TableBuilder, TableReader};
use crate::memtable::MemTable;

pub struct LocalStorage {
    fs: Box<dyn FileSystem>,
    next_number: AtomicU64,
    version: Mutex<LocalStorageVersion>,
    current: Mutex<StorageVersionRef>,
    current_tx: StorageVersionSender,
    current_rx: StorageVersionReceiver,
}

impl LocalStorage {
    pub fn new(fs: Box<dyn FileSystem>) -> Result<LocalStorage> {
        let version = LocalStorageVersion::new();
        let current = version.into_ref();
        let (tx, rx) = watch::channel(current.clone());
        Ok(LocalStorage {
            fs,
            next_number: AtomicU64::new(0),
            version: Mutex::new(version),
            current: Mutex::new(current),
            current_tx: tx,
            current_rx: rx,
        })
    }

    async fn install(&self, edit: VersionEdit) -> Result<StorageVersionRef> {
        let mut version = self.version.lock().await;
        match edit {
            VersionEdit::Flush(meta) => {
                let file = self.fs.new_random_access_reader(&meta.file_name).await?;
                let reader = SstReader::open(file, meta.file_size).await?;
                version.add_file(meta, reader);
            }
        }
        let mut current = self.current.lock().await;
        *current = version.into_ref();
        Ok(current.clone())
    }
}

#[async_trait]
impl Storage for LocalStorage {
    async fn current(&self) -> StorageVersionRef {
        self.current.lock().await.clone()
    }

    fn current_rx(&self) -> StorageVersionReceiver {
        self.current_rx.clone()
    }

    async fn flush_memtable(&self, mem: Arc<Box<dyn MemTable>>) -> Result<StorageVersionRef> {
        let options = SstOptions::default();
        let file_number = self.next_number.fetch_add(1, Ordering::SeqCst);
        let fname = format!("{}.sst", file_number);
        let file = self.fs.new_sequential_writer(&fname).await?;
        let mut builder = SstBuilder::new(options, file);
        let snap = mem.snapshot().await;
        let mut iter = snap.iter();
        for v in iter.next() {
            builder.add(v.0, v.1, v.2).await;
        }
        let file_size = builder.finish().await?;
        let file_meta = FileMeta {
            file_name: fname,
            file_size,
        };
        let edit = VersionEdit::Flush(file_meta);
        self.install(edit).await
    }
}

#[derive(Clone, Debug)]
pub struct FileMeta {
    file_name: String,
    file_size: usize,
}

pub enum VersionEdit {
    Flush(FileMeta),
}

pub struct LocalStorageVersion {
    levels: Vec<FileMeta>,
    readers: Vec<Arc<SstReader>>,
}

impl LocalStorageVersion {
    fn new() -> LocalStorageVersion {
        LocalStorageVersion {
            levels: Vec::new(),
            readers: Vec::new(),
        }
    }

    fn add_file(&mut self, meta: FileMeta, reader: SstReader) {
        self.levels.push(meta);
        self.readers.push(Arc::new(reader));
    }

    fn into_ref(&self) -> StorageVersionRef {
        Arc::new(Box::new(LocalStorageVersion {
            levels: self.levels.clone(),
            readers: self.readers.clone(),
        }))
    }
}

#[async_trait]
impl StorageVersion for LocalStorageVersion {
    async fn get(&self, ts: Timestamp, key: &[u8]) -> Result<Option<Vec<u8>>> {
        for reader in self.readers.iter().rev() {
            if let Some(v) = reader.get(ts, key).await? {
                return Ok(Some(v));
            }
        }
        Ok(None)
    }
}
