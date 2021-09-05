use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::{watch, Mutex};
use tokio::task;
use tokio::time::{self, Duration};

use super::{
    Storage, StorageOptions, StorageVersion, StorageVersionReceiver, StorageVersionSender,
};
use crate::error::Result;
use crate::file_system::FileSystem;
use crate::format::{
    sst_name, FileDesc, SstBuilder, SstOptions, SstReader, TableBuilder, TableReader, Timestamp,
};
use crate::manifest::{Manifest, VersionDesc};
use crate::memtable::MemTable;

pub struct LocalStorage {
    options: StorageOptions,
    fs: Arc<dyn FileSystem>,
    manifest: Arc<dyn Manifest>,
    handle: Arc<VersionHandle>,
    current_rx: StorageVersionReceiver,
}

impl LocalStorage {
    pub async fn new(
        options: StorageOptions,
        fs: Arc<dyn FileSystem>,
        manifest: Arc<dyn Manifest>,
    ) -> Result<LocalStorage> {
        let handle = Arc::new(VersionHandle::new(fs.clone()));
        let version = manifest.current().await?;
        let current = handle.install_version(version).await?;
        let (tx, rx) = watch::channel(current);
        let handle_clone = handle.clone();
        let manifest_clone = manifest.clone();
        task::spawn(async move {
            update_manifest(handle_clone, manifest_clone, tx).await;
        });
        Ok(LocalStorage {
            options,
            fs,
            manifest,
            handle,
            current_rx: rx,
        })
    }
}

#[async_trait]
impl Storage for LocalStorage {
    async fn current(&self) -> Arc<dyn StorageVersion> {
        self.handle.current().await
    }

    fn current_rx(&self) -> StorageVersionReceiver {
        self.current_rx.clone()
    }

    async fn flush_memtable(&self, mem: Arc<dyn MemTable>) -> Result<Arc<dyn StorageVersion>> {
        let options = SstOptions {
            block_size: self.options.block_size,
        };
        let file_number = self.manifest.next_number().await?;
        let file_name = sst_name(file_number);
        let file_writer = self.fs.new_sequential_writer(&file_name).await?;
        let mut builder = SstBuilder::new(options, file_writer);
        let snap = mem.snapshot().await;
        for v in snap.iter() {
            builder.add(v.0, v.1, v.2).await
        }
        let file_size = builder.finish().await?;
        let file_desc = FileDesc {
            file_number,
            file_size: file_size as u64,
        };
        let version = self.manifest.install_flush(file_desc).await?;
        let current = self.handle.install_version(version).await?;
        Ok(current)
    }
}

struct File {
    desc: FileDesc,
    reader: SstReader,
}

#[derive(Clone)]
struct Version {
    files: Vec<Arc<File>>,
}

impl Version {
    fn new() -> Version {
        Version { files: Vec::new() }
    }

    fn make_storage_version(&self) -> Arc<dyn StorageVersion> {
        Arc::new(Version {
            files: self.files.clone(),
        })
    }
}

#[async_trait]
impl StorageVersion for Version {
    async fn get(&self, ts: Timestamp, key: &[u8]) -> Result<Option<Vec<u8>>> {
        for file in self.files.iter() {
            if let Some(v) = file.reader.get(ts, key).await? {
                return Ok(Some(v));
            }
        }
        Ok(None)
    }
}

struct VersionHandle {
    fs: Arc<dyn FileSystem>,
    version: Mutex<Version>,
    current: Mutex<Arc<dyn StorageVersion>>,
}

impl VersionHandle {
    fn new(fs: Arc<dyn FileSystem>) -> VersionHandle {
        let version = Version::new();
        let current = version.make_storage_version();
        VersionHandle {
            fs,
            version: Mutex::new(version),
            current: Mutex::new(current),
        }
    }

    async fn current(&self) -> Arc<dyn StorageVersion> {
        self.current.lock().await.clone()
    }

    async fn open_file(&self, desc: FileDesc) -> Result<File> {
        let file_name = sst_name(desc.file_number);
        let file_reader = self.fs.new_random_access_reader(&file_name).await?;
        let reader = SstReader::open(file_reader, desc.file_size).await?;
        Ok(File { desc, reader })
    }

    async fn install_version(&self, desc: VersionDesc) -> Result<Arc<dyn StorageVersion>> {
        let mut new_version = Version::new();
        let old_version = self.version.lock().await.clone();
        for desc in desc.files {
            if let Some(file) = old_version
                .files
                .iter()
                .find(|x| x.desc.file_number == desc.file_number)
            {
                new_version.files.push(file.clone());
            } else {
                let file = self.open_file(desc).await?;
                new_version.files.push(Arc::new(file));
            }
        }
        let mut version = self.version.lock().await;
        *version = new_version;
        let mut current = self.current.lock().await;
        *current = version.make_storage_version();
        Ok(current.clone())
    }
}

async fn update_manifest(
    handle: Arc<VersionHandle>,
    manifest: Arc<dyn Manifest>,
    current_tx: StorageVersionSender,
) {
    let mut interval = time::interval(Duration::from_secs(1));
    loop {
        interval.tick().await;
        let version = manifest.current().await.unwrap();
        let current = handle.install_version(version).await.unwrap();
        if current_tx.send(current).is_err() {
            eprintln!("send current error");
        }
    }
}
