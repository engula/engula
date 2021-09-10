use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

use tokio::{
    sync::{mpsc, oneshot, Mutex, RwLock},
    task, time,
};

use crate::{
    error::Result,
    format::Timestamp,
    journal::Journal,
    manifest::Manifest,
    memtable::{BTreeTable, MemTable},
    storage::Storage,
    version_set::{Version, VersionSet},
    write::{BatchReceiver, Write},
};

#[derive(Clone)]
pub struct Options {
    pub memtable_size: usize,
    pub write_batch_size: usize,
    pub write_channel_size: usize,
}

impl Options {
    pub fn default() -> Options {
        Options {
            memtable_size: 4 * 1024,
            write_batch_size: 4 * 1024,
            write_channel_size: 4 * 1024,
        }
    }
}

pub struct Database {
    core: Arc<Core>,
    next_ts: AtomicU64,
    write_tx: mpsc::Sender<Write>,
}

impl Database {
    pub async fn new(
        options: Options,
        journal: Arc<dyn Journal>,
        storage: Arc<dyn Storage>,
        manifest: Arc<dyn Manifest>,
    ) -> Result<Database> {
        let core = Core::new(options.clone(), journal, storage, manifest).await?;
        let core = Arc::new(core);
        let core_clone = core.clone();
        let (write_tx, write_rx) = mpsc::channel(options.write_channel_size);
        task::spawn(async move {
            core_clone.write(write_rx).await;
        });
        Ok(Database {
            core,
            next_ts: AtomicU64::new(0),
            write_tx,
        })
    }

    pub async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let ts = self.next_ts.load(Ordering::SeqCst);
        self.core.get(ts, key).await
    }

    pub async fn put(&self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        let ts = self.next_ts.fetch_add(1, Ordering::SeqCst);
        let (tx, rx) = oneshot::channel();
        let write = Write { tx, ts, key, value };
        self.write_tx.send(write).await?;
        rx.await?;
        Ok(())
    }
}

struct Core {
    options: Options,
    journal: Arc<dyn Journal>,
    super_handle: Arc<SuperVersionHandle>,
    flush_handle: Mutex<Option<task::JoinHandle<()>>>,
}

impl Core {
    async fn new(
        options: Options,
        journal: Arc<dyn Journal>,
        storage: Arc<dyn Storage>,
        manifest: Arc<dyn Manifest>,
    ) -> Result<Core> {
        let vset = VersionSet::new(storage, manifest).await;
        let super_handle = SuperVersionHandle::new(vset).await?;
        let super_handle = Arc::new(super_handle);
        let super_handle_clone = super_handle.clone();
        task::spawn(async move {
            update_version(super_handle_clone).await;
        });
        Ok(Core {
            options,
            journal,
            super_handle,
            flush_handle: Mutex::new(None),
        })
    }

    async fn get(&self, ts: Timestamp, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.super_handle.get(ts, key).await
    }

    async fn write(&self, rx: mpsc::Receiver<Write>) {
        let mut batch_rx = BatchReceiver::new(rx, self.options.write_batch_size);
        while let Some(writes) = batch_rx.recv().await {
            let mut data = Vec::new();
            for write in &writes {
                write.encode_to(&mut data);
            }
            self.journal.append(data).await.unwrap();
            for write in writes {
                let memtable_size = self
                    .super_handle
                    .put(write.ts, write.key, write.value)
                    .await;
                write.tx.send(()).unwrap();
                if memtable_size >= self.options.memtable_size {
                    self.flush_memtable().await;
                }
            }
        }
    }

    async fn flush_memtable(&self) {
        let mut flush_handle = self.flush_handle.lock().await;
        if let Some(handle) = flush_handle.take() {
            handle.await.unwrap();
        }
        let super_handle = self.super_handle.clone();
        let handle = task::spawn(async move {
            super_handle.flush_memtable().await.unwrap();
        });
        *flush_handle = Some(handle);
    }
}

struct SuperVersion {
    mem: Arc<dyn MemTable>,
    imm: Option<Arc<dyn MemTable>>,
    version: Arc<Version>,
}

struct SuperVersionHandle {
    vset: VersionSet,
    current: RwLock<Arc<SuperVersion>>,
    sequence: Mutex<u64>,
}

impl SuperVersionHandle {
    async fn new(vset: VersionSet) -> Result<SuperVersionHandle> {
        let version = vset.current().await?;
        let current = SuperVersion {
            mem: Arc::new(BTreeTable::new()),
            imm: None,
            version,
        };
        Ok(SuperVersionHandle {
            vset,
            current: RwLock::new(Arc::new(current)),
            sequence: Mutex::new(0),
        })
    }

    async fn get(&self, ts: Timestamp, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let current = self.current.read().await.clone();
        if let Some(value) = current.mem.get(ts, key).await {
            return Ok(Some(value));
        }
        if let Some(imm) = &current.imm {
            if let Some(value) = imm.get(ts, key).await {
                return Ok(Some(value));
            }
        }
        current.version.get(ts, key).await
    }

    async fn put(&self, ts: Timestamp, key: Vec<u8>, value: Vec<u8>) -> usize {
        let current = self.current.read().await.clone();
        current.mem.put(ts, key, value).await;
        current.mem.approximate_size()
    }

    async fn flush_memtable(&self) -> Result<()> {
        let imm = self.switch_memtable().await;
        let version = self.vset.flush_memtable(imm).await?;
        self.install_flush_version(version).await;
        Ok(())
    }

    async fn switch_memtable(&self) -> Arc<dyn MemTable> {
        let mut current = self.current.write().await;
        assert!(current.imm.is_none());
        *current = Arc::new(SuperVersion {
            mem: Arc::new(BTreeTable::new()),
            imm: Some(current.mem.clone()),
            version: current.version.clone(),
        });
        current.imm.clone().unwrap()
    }

    async fn version_updated(&self, version: Arc<Version>) -> bool {
        let mut sequence = self.sequence.lock().await;
        if *sequence >= version.sequence() {
            false
        } else {
            *sequence = version.sequence();
            true
        }
    }

    async fn install_version(&self, version: Arc<Version>) {
        if !self.version_updated(version.clone()).await {
            return;
        }
        let mut current = self.current.write().await;
        *current = Arc::new(SuperVersion {
            mem: current.mem.clone(),
            imm: current.imm.clone(),
            version,
        });
    }

    async fn install_flush_version(&self, version: Arc<Version>) {
        if !self.version_updated(version.clone()).await {
            return;
        }
        let mut current = self.current.write().await;
        assert!(current.imm.is_some());
        *current = Arc::new(SuperVersion {
            mem: current.mem.clone(),
            imm: None,
            version,
        });
    }
}

async fn update_version(handle: Arc<SuperVersionHandle>) {
    let mut interval = time::interval(time::Duration::from_secs(1));
    loop {
        interval.tick().await;
        let version = handle.vset.current().await.unwrap();
        handle.install_version(version).await;
    }
}
