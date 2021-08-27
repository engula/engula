use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;

use tokio::sync::{mpsc, oneshot, Mutex, RwLock};
use tokio::task;

use crate::common::Timestamp;
use crate::journal::Journal;
use crate::memtable::{BTreeTable, Memtable};
use crate::storage::{Storage, StorageVersion};
use crate::Result;

pub struct Options {
    pub memtable_size: usize,
}

pub struct Database {
    core: Arc<Core>,
    last_ts: AtomicU64,
    write_tx: mpsc::Sender<Put>,
    write_thread: thread::JoinHandle<()>,
}

impl Database {
    pub fn new(options: Options, journal: Box<dyn Journal>, storage: Box<dyn Storage>) -> Database {
        let core = Arc::new(Core::new(options, journal, storage));
        let core_clone = core.clone();
        let (write_tx, write_rx) = mpsc::channel(4096);
        let write_thread = thread::spawn(move || {
            let _ = core_clone.handle_writes(write_rx);
        });
        Database {
            core,
            last_ts: AtomicU64::new(0),
            write_tx,
            write_thread,
        }
    }

    pub async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let ts = self.last_ts.load(Ordering::SeqCst);
        self.core.get(ts, key).await
    }

    pub async fn put(&self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        let ts = self.last_ts.fetch_add(1, Ordering::SeqCst);
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
    journal: Box<dyn Journal>,
    storage: Box<dyn Storage>,
    current: Arc<VersionHandle>,
    pending_flush: Mutex<Option<task::JoinHandle<()>>>,
}

impl Core {
    fn new(options: Options, journal: Box<dyn Journal>, storage: Box<dyn Storage>) -> Core {
        let version = storage.current();
        Core {
            options,
            journal,
            storage,
            current: Arc::new(VersionHandle::new(version)),
            pending_flush: Mutex::new(None),
        }
    }

    async fn get(&self, ts: Timestamp, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.current.get(ts, key).await
    }

    #[tokio::main]
    async fn handle_writes(&self, mut rx: mpsc::Receiver<Put>) -> Result<()> {
        while let Some(put) = rx.recv().await {
            let data = put.encode();
            self.journal.append(data).await?;
            put.tx.send(()).unwrap();
            let memtable_size = self.current.put(put.ts, put.key, put.value).await;
            if memtable_size >= self.options.memtable_size {
                self.schedule_flush().await;
            }
        }
        Ok(())
    }

    async fn schedule_flush(&self) -> Result<()> {
        let mut pending_flush = self.pending_flush.lock().await;
        if let Some(handle) = pending_flush.take() {
            handle.await?;
        }

        let current = self.current.clone();
        current.switch_memtable();

        let handle = task::spawn(async move {
            // flush(imm);
            current.release_immtable();
        });
        *pending_flush = Some(handle);

        Ok(())
    }
}

struct Version {
    mem: Arc<Box<dyn Memtable>>,
    imm: Option<Arc<Box<dyn Memtable>>>,
    storage: Arc<Box<dyn StorageVersion>>,
}

struct VersionHandle(RwLock<Arc<Version>>);

impl VersionHandle {
    fn new(storage: Arc<Box<dyn StorageVersion>>) -> VersionHandle {
        let version = Version {
            mem: Arc::new(Box::new(BTreeTable::new())),
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
        current.mem.insert(ts, key, value).await;
        current.mem.approximate_size()
    }

    async fn switch_memtable(&self) {
        let mut current = self.0.write().await;
        let version = Arc::new(Version {
            mem: Arc::new(Box::new(BTreeTable::new())),
            imm: Some(current.mem.clone()),
            storage: current.storage.clone(),
        });
        *current = version;
    }

    async fn release_immtable(&self) {
        let mut current = self.0.write().await;
        let version = Arc::new(Version {
            mem: current.mem.clone(),
            imm: None,
            storage: current.storage.clone(),
        });
        *current = version;
    }
}
