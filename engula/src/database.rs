use std::{
    collections::hash_map::DefaultHasher,
    hash::Hasher,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use tokio::{
    sync::{mpsc, oneshot, Mutex, RwLock},
    task, time,
};
use tracing::error;

use crate::{
    error::Result,
    format::Timestamp,
    journal::Journal,
    manifest::Manifest,
    memtable::{BTreeTable, MemTable},
    storage::Storage,
    version_set::{Version, VersionSet},
    write::{Write, WriteBatch, WriteBatchReceiver, WriteReceiver},
};

#[derive(Clone)]
pub struct Options {
    pub num_cores: usize,
    pub memtable_size: usize,
    pub write_batch_size: usize,
    pub write_channel_size: usize,
}

impl Options {
    pub fn default() -> Options {
        Options {
            num_cores: 4,
            memtable_size: 4 * 1024,
            write_batch_size: 4 * 1024,
            write_channel_size: 4 * 1024,
        }
    }
}

pub struct Database {
    cores: Vec<Arc<Core>>,
    next_ts: AtomicU64,
}

impl Database {
    pub async fn new(
        options: Options,
        journal: Arc<dyn Journal>,
        storage: Arc<dyn Storage>,
        manifest: Arc<dyn Manifest>,
    ) -> Result<Database> {
        let (journal_tx, journal_rx) = mpsc::channel(options.write_channel_size);
        let journal_tx = Arc::new(journal_tx);

        let mut cores = Vec::new();
        for i in 0..options.num_cores {
            let (write_tx, write_rx) = mpsc::channel(options.write_channel_size);
            let (memtable_tx, memtable_rx) = mpsc::channel(options.write_channel_size);
            let vset = VersionSet::new(i as u64, storage.clone(), manifest.clone());
            let core = Core::new(options.clone(), write_tx, memtable_tx, vset).await?;
            let core = Arc::new(core);

            // Spawns a task to handle writes per core.
            let core_clone = core.clone();
            let journal_tx_clone = journal_tx.clone();
            task::spawn(async move {
                core_clone.write_batch(write_rx, journal_tx_clone).await;
            });

            // Spawns a task to handle memtable writes per core.
            let core_clone = core.clone();
            task::spawn(async move {
                core_clone.write_memtable(memtable_rx).await;
            });

            cores.push(core);
        }

        // Spawns a task to handle journal writes per database.
        task::spawn(async move {
            write_journal(options, journal, journal_rx).await;
        });

        Ok(Database {
            cores,
            next_ts: AtomicU64::new(0),
        })
    }

    pub async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let ts = self.next_ts.load(Ordering::SeqCst);
        let core = self.select_core(key);
        core.get(ts, key).await
    }

    pub async fn put(&self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        let ts = self.next_ts.fetch_add(1, Ordering::SeqCst);
        let core = self.select_core(&key);
        core.put(ts, key, value).await
    }

    fn select_core(&self, key: &[u8]) -> Arc<Core> {
        let mut hasher = DefaultHasher::new();
        hasher.write(key);
        let hash = hasher.finish() as usize;
        self.cores[hash % self.cores.len()].clone()
    }
}

struct Core {
    options: Options,
    write_tx: mpsc::Sender<Write>,
    memtable_tx: Arc<mpsc::Sender<Vec<Write>>>,
    super_handle: Arc<SuperVersionHandle>,
    flush_handle: Mutex<Option<task::JoinHandle<()>>>,
}

impl Core {
    async fn new(
        options: Options,
        write_tx: mpsc::Sender<Write>,
        memtable_tx: mpsc::Sender<Vec<Write>>,
        version_set: VersionSet,
    ) -> Result<Core> {
        let super_handle = SuperVersionHandle::new(version_set).await?;
        let super_handle = Arc::new(super_handle);
        let super_handle_clone = super_handle.clone();
        task::spawn(async move {
            update_version(super_handle_clone).await;
        });
        Ok(Core {
            options,
            write_tx,
            memtable_tx: Arc::new(memtable_tx),
            super_handle,
            flush_handle: Mutex::new(None),
        })
    }

    async fn get(&self, ts: Timestamp, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.super_handle.get(ts, key).await
    }

    async fn put(&self, ts: Timestamp, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        let write = Write { tx, ts, key, value };
        self.write_tx.send(write).await?;
        rx.await?;
        Ok(())
    }

    async fn write_batch(&self, rx: mpsc::Receiver<Write>, tx: Arc<mpsc::Sender<WriteBatch>>) {
        let mut batch_rx = WriteReceiver::new(rx, self.options.write_batch_size);
        while let Some(writes) = batch_rx.recv().await {
            let mut buffer = Vec::with_capacity(self.options.write_batch_size);
            for write in &writes {
                write.encode_to(&mut buffer);
            }
            let batch = WriteBatch {
                tx: self.memtable_tx.clone(),
                writes,
                buffer,
            };
            tx.send(batch).await.unwrap();
        }
    }

    async fn write_memtable(&self, mut rx: mpsc::Receiver<Vec<Write>>) {
        while let Some(writes) = rx.recv().await {
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
        let imm = super_handle.switch_memtable().await;
        let handle = task::spawn(async move {
            super_handle.flush_memtable(imm.clone()).await.unwrap();
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

    async fn flush_memtable(&self, mem: Arc<dyn MemTable>) -> Result<()> {
        let version = self.vset.flush_memtable(mem).await?;
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

async fn write_journal(
    options: Options,
    journal: Arc<dyn Journal>,
    rx: mpsc::Receiver<WriteBatch>,
) {
    let mut batch_rx = WriteBatchReceiver::new(rx, options.write_batch_size);
    while let Some(batch) = batch_rx.recv().await {
        if let Err(err) = journal.append(batch.buffer).await {
            error!("write journal: {}", err);
        }
        for batch in batch.writes {
            batch.tx.send(batch.writes).await.unwrap();
        }
    }
}

async fn update_version(handle: Arc<SuperVersionHandle>) {
    let mut interval = time::interval(time::Duration::from_secs(1));
    loop {
        interval.tick().await;
        if let Ok(version) = handle.vset.current().await {
            handle.install_version(version).await;
        }
    }
}
