use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use tokio::sync::{mpsc, oneshot, Mutex, RwLock};
use tokio::task;
use tokio_stream::{wrappers::WatchStream, StreamExt};

use crate::common::Timestamp;
use crate::error::Result;
use crate::journal::Journal;
use crate::memtable::{BTreeTable, MemTable};
use crate::storage::{Storage, StorageVersion, StorageVersionReceiver, StorageVersionRef};

pub struct Options {
    pub memtable_size: usize,
}

impl Options {
    pub fn default() -> Options {
        Options {
            memtable_size: 1024 * 1024,
        }
    }
}

pub struct Database {
    core: Arc<Core>,
    next_ts: AtomicU64,
    write_tx: mpsc::Sender<Put>,
}

impl Database {
    pub async fn new(
        options: Options,
        journal: Arc<dyn Journal>,
        storage: Arc<dyn Storage>,
    ) -> Database {
        let core = Core::new(options, journal, storage).await;
        let core = Arc::new(core);
        let core_clone = core.clone();
        let (write_tx, write_rx) = mpsc::channel(4096);
        task::spawn(async move {
            let _ = core_clone.handle_writes(write_rx).await;
        });
        Database {
            core,
            next_ts: AtomicU64::new(0),
            write_tx,
        }
    }

    pub async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let ts = self.next_ts.load(Ordering::SeqCst);
        self.core.get(ts, key).await
    }

    pub async fn put(&self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        let ts = self.next_ts.fetch_add(1, Ordering::SeqCst);
        let put = Put { tx, ts, key, value };
        self.write_tx.send(put).await?;
        rx.await?;
        Ok(())
    }
}

#[derive(Debug)]
struct Put {
    tx: oneshot::Sender<()>,
    ts: Timestamp,
    key: Vec<u8>,
    value: Vec<u8>,
}

impl Put {
    fn encode(&self) -> Vec<u8> {
        let mut buf =
            Vec::with_capacity(std::mem::size_of_val(&self.ts) + self.key.len() + self.value.len());
        buf.extend_from_slice(&self.ts.to_le_bytes());
        buf.extend_from_slice(&self.key);
        buf.extend_from_slice(&self.value);
        buf
    }
}

struct Core {
    options: Options,
    journal: Arc<dyn Journal>,
    storage: Arc<dyn Storage>,
    current: Arc<VersionHandle>,
    flush_handle: Mutex<Option<task::JoinHandle<()>>>,
}

impl Core {
    async fn new(options: Options, journal: Arc<dyn Journal>, storage: Arc<dyn Storage>) -> Core {
        let storage_version = storage.current().await;
        let current = Arc::new(VersionHandle::new(storage_version));
        let current_clone = current.clone();
        let current_rx = storage.current_rx();
        task::spawn(async move {
            VersionHandle::watch_storage_version(current_clone, current_rx)
                .await
                .unwrap();
        });
        Core {
            options,
            journal,
            storage,
            current,
            flush_handle: Mutex::new(None),
        }
    }

    async fn get(&self, ts: Timestamp, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.current.get(ts, key).await
    }

    async fn handle_writes(&self, mut rx: mpsc::Receiver<Put>) -> Result<()> {
        while let Some(put) = rx.recv().await {
            let data = put.encode();
            self.journal.append(data).await?;
            let memtable_size = self.current.put(put.ts, put.key, put.value).await;
            put.tx.send(()).unwrap();
            if memtable_size >= self.options.memtable_size {
                self.schedule_flush().await?;
            }
        }
        Ok(())
    }

    async fn schedule_flush(&self) -> Result<()> {
        let mut flush_handle = self.flush_handle.lock().await;
        if let Some(handle) = flush_handle.take() {
            handle.await?;
        }

        let storage = self.storage.clone();
        let current = self.current.clone();
        let imm = current.switch_memtable().await;

        let handle = task::spawn(async move {
            let version = storage.flush_memtable(imm).await.unwrap();
            current.install_flush_result(version).await;
        });
        *flush_handle = Some(handle);

        Ok(())
    }
}

struct Version {
    mem: Arc<dyn MemTable>,
    imm: Option<Arc<dyn MemTable>>,
    storage: StorageVersionRef,
}

struct VersionHandle(RwLock<Arc<Version>>);

impl VersionHandle {
    fn new(storage: Arc<dyn StorageVersion>) -> VersionHandle {
        let version = Version {
            mem: Arc::new(BTreeTable::new()),
            imm: None,
            storage,
        };
        VersionHandle(RwLock::new(Arc::new(version)))
    }

    async fn get(&self, ts: Timestamp, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let current = self.0.read().await.clone();
        if let Some(value) = current.mem.get(ts, key).await {
            return Ok(Some(value));
        }
        if let Some(imm) = &current.imm {
            if let Some(value) = imm.get(ts, key).await {
                return Ok(Some(value));
            }
        }
        current.storage.get(ts, key).await
    }

    async fn put(&self, ts: Timestamp, key: Vec<u8>, value: Vec<u8>) -> usize {
        let current = self.0.read().await.clone();
        current.mem.put(ts, key, value).await;
        current.mem.approximate_size()
    }

    async fn switch_memtable(&self) -> Arc<dyn MemTable> {
        let mut current = self.0.write().await;
        assert!(current.imm.is_none());
        let version = Arc::new(Version {
            mem: Arc::new(BTreeTable::new()),
            imm: Some(current.mem.clone()),
            storage: current.storage.clone(),
        });
        *current = version;
        current.imm.clone().unwrap()
    }

    async fn install_flush_result(&self, storage: StorageVersionRef) {
        let mut current = self.0.write().await;
        assert!(current.imm.is_some());
        let version = Arc::new(Version {
            mem: current.mem.clone(),
            imm: None,
            storage,
        });
        *current = version;
    }

    async fn watch_storage_version(
        current: Arc<VersionHandle>,
        current_rx: StorageVersionReceiver,
    ) -> Result<()> {
        let mut rx = WatchStream::new(current_rx);
        while let Some(version) = rx.next().await {
            current.install_storage_version(version).await;
        }
        Ok(())
    }

    async fn install_storage_version(&self, storage: StorageVersionRef) {
        let mut current = self.0.write().await;
        let version = Arc::new(Version {
            mem: current.mem.clone(),
            imm: current.imm.clone(),
            storage,
        });
        *current = version;
    }
}
