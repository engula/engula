use std::{
    collections::hash_map::DefaultHasher,
    hash::Hasher,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use futures::StreamExt;
use metrics::histogram;
use tokio::{
    sync::{mpsc, oneshot, Mutex, RwLock},
    task,
    time::{self, Duration, Instant},
};
use tokio_stream::wrappers::ReceiverStream;
use tracing::{error, warn};

use crate::{
    cache::{Cache, LruCache},
    error::Result,
    format::{TableReaderOptions, Timestamp},
    journal::{Journal, Write, WriteBatch},
    manifest::Manifest,
    memtable::{BTreeTable, MemTable},
    storage::Storage,
    version_set::{Version, VersionSet},
};

#[derive(Clone, Debug)]
pub struct Options {
    pub num_shards: usize,
    pub cache_size: usize,
    pub memtable_size: usize,
    pub write_channel_size: usize,
}

impl Options {
    pub fn default() -> Options {
        Options {
            num_shards: 8,
            cache_size: 0,
            memtable_size: 16 * 1024,
            write_channel_size: 1024,
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
        let (drop_memtable_tx, drop_memtable_rx) = mpsc::channel(options.write_channel_size);

        let mut cores = Vec::new();
        for i in 0..options.num_shards {
            let mut options = options.clone();
            options.cache_size /= options.num_shards;
            options.memtable_size /= options.num_shards;
            let (write_tx, write_rx) = mpsc::channel(options.write_channel_size);
            let (memtable_tx, memtable_rx) = mpsc::channel(options.write_channel_size);
            let core = Core::new(
                i as u64,
                options,
                storage.clone(),
                manifest.clone(),
                write_tx,
                memtable_tx,
                drop_memtable_tx.clone(),
            )
            .await?;
            let core = Arc::new(core);

            // Spawns a task to handle writes per core.
            let core_clone = core.clone();
            let journal_tx = journal_tx.clone();
            task::spawn(async move {
                core_clone.write_batch(write_rx, journal_tx).await;
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
            if let Err(err) = journal.append_stream(journal_rx).await {
                error!("append journal: {}", err);
            }
        });
        // Spawns a task to drop memtables in the background.
        task::spawn(async move {
            drop_memtable(drop_memtable_rx).await;
        });

        Ok(Database {
            cores,
            next_ts: AtomicU64::new(0),
        })
    }

    pub async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let core = self.select_core(key);
        core.get(u64::MAX, key).await
    }

    pub async fn timed_get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let start = Instant::now();
        let value = self.get(key).await?;
        histogram!("engula.get.us", start.elapsed().as_micros() as f64);
        Ok(value)
    }

    pub async fn put(&self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        let ts = self.next_ts.fetch_add(1, Ordering::SeqCst);
        let core = self.select_core(&key);
        core.put(ts, key, value).await
    }

    pub async fn timed_put(&self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        let start = Instant::now();
        self.put(key, value).await?;
        histogram!("engula.put.us", start.elapsed().as_micros() as f64);
        Ok(())
    }

    pub async fn count(&self) -> Result<usize> {
        let mut sum = 0;
        for core in &self.cores {
            sum += core.count().await?;
        }
        Ok(sum)
    }

    pub async fn flush(&self) {
        for core in &self.cores {
            core.flush_memtable(true).await;
        }
    }

    fn select_core(&self, key: &[u8]) -> &Core {
        let mut hasher = DefaultHasher::new();
        hasher.write(key);
        let hash = hasher.finish() as usize;
        &self.cores[hash % self.cores.len()]
    }
}

struct Core {
    name: String,
    options: Options,
    write_tx: mpsc::Sender<Write>,
    memtable_tx: Arc<mpsc::Sender<Vec<Write>>>,
    super_handle: Arc<SuperVersionHandle>,
    flush_handle: Mutex<Option<task::JoinHandle<()>>>,
}

impl Core {
    async fn new(
        id: u64,
        options: Options,
        storage: Arc<dyn Storage>,
        manifest: Arc<dyn Manifest>,
        write_tx: mpsc::Sender<Write>,
        memtable_tx: mpsc::Sender<Vec<Write>>,
        drop_memtable_tx: mpsc::Sender<Arc<dyn MemTable>>,
    ) -> Result<Core> {
        let cache = if options.cache_size == 0 {
            None
        } else {
            let cache: Arc<dyn Cache> = Arc::new(LruCache::new(options.cache_size, 32));
            Some(cache)
        };
        let table_options = TableReaderOptions {
            cache,
            prefetch: false,
        };
        let vset = VersionSet::new(id, storage.clone(), manifest.clone(), table_options).await?;
        let super_handle = SuperVersionHandle::new(vset, drop_memtable_tx).await?;
        let super_handle = Arc::new(super_handle);
        let super_handle_clone = super_handle.clone();
        task::spawn(async move {
            update_version(super_handle_clone).await;
        });
        Ok(Core {
            name: format!("shard:{}", id),
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

    async fn count(&self) -> Result<usize> {
        self.super_handle.count().await
    }

    async fn write_batch(&self, rx: mpsc::Receiver<Write>, tx: mpsc::Sender<WriteBatch>) {
        let mut stream = ReceiverStream::new(rx).ready_chunks(self.options.write_channel_size);
        while let Some(writes) = stream.next().await {
            let mut buffer = Vec::with_capacity(1024 * 1024);
            for write in &writes {
                write.encode_to(&mut buffer);
            }
            let batch = WriteBatch {
                tx: self.memtable_tx.clone(),
                buffer,
                writes,
            };
            tx.send(batch).await.unwrap();
            // Gives way to clients.
            task::yield_now().await;
        }
    }

    async fn write_memtable(&self, mut rx: mpsc::Receiver<Vec<Write>>) {
        while let Some(writes) = rx.recv().await {
            let memtable_size = self.super_handle.write(writes).await;
            if memtable_size >= self.options.memtable_size / 2 {
                self.flush_memtable(false).await;
            }
        }
    }

    async fn flush_memtable(&self, wait: bool) {
        let mut flush_handle = self.flush_handle.lock().await;
        if let Some(handle) = flush_handle.take() {
            let start = Instant::now();
            handle.await.unwrap();
            let elapsed = start.elapsed();
            if elapsed >= Duration::from_millis(1) {
                warn!("[{}] stop writes for {:?}", self.name, start.elapsed());
            }
        }

        let shard_name = self.name.clone();
        let super_handle = self.super_handle.clone();
        let imm = super_handle.switch_memtable().await;
        let handle = task::spawn(async move {
            if let Err(err) = super_handle.flush_memtable(imm.clone()).await {
                error!(
                    "[{}] flush memtable size {}: {}",
                    shard_name,
                    imm.size(),
                    err
                );
            }
        });

        if wait {
            handle.await.unwrap();
        } else {
            *flush_handle = Some(handle);
        }
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
    drop_memtable_tx: mpsc::Sender<Arc<dyn MemTable>>,
}

impl SuperVersionHandle {
    async fn new(
        vset: VersionSet,
        drop_memtable_tx: mpsc::Sender<Arc<dyn MemTable>>,
    ) -> Result<SuperVersionHandle> {
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
            drop_memtable_tx,
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

    async fn write(&self, writes: Vec<Write>) -> usize {
        let current = self.current.read().await.clone();
        for write in writes {
            current.mem.put(write.ts, write.key, write.value).await;
            write.tx.send(()).unwrap();
        }
        current.mem.size()
    }

    async fn count(&self) -> Result<usize> {
        let current = self.current.read().await.clone();
        let mut sum = current.mem.count();
        if let Some(imm) = &current.imm {
            sum += imm.count();
        }
        sum += current.version.count().await?;
        Ok(sum)
    }

    async fn flush_memtable(&self, mem: Arc<dyn MemTable>) -> Result<()> {
        // This may be the last reference of the memtable.
        // We clone it here to prevent dropping it inside the lock.
        let version = self.vset.flush_memtable(mem.clone()).await?;
        self.install_flush_version(version).await;
        let _ = self.drop_memtable_tx.send(mem).await;
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
    let mut interval = time::interval(Duration::from_secs(1));
    loop {
        interval.tick().await;
        match handle.vset.current().await {
            Ok(version) => handle.install_version(version).await,
            Err(err) => error!("current: {}", err),
        }
    }
}

async fn drop_memtable(mut rx: mpsc::Receiver<Arc<dyn MemTable>>) {
    while let Some(mem) = rx.recv().await {
        // Waits until all references have been dropped.
        time::sleep(Duration::from_millis(10)).await;
        // Dropping the memtable takes too long and blocks other tasks,
        // this is a dirty workaround only applied to the demo.
        std::mem::forget(mem);
    }
}
