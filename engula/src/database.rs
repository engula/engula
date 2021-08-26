use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;

use tokio::sync::{mpsc, oneshot, RwLock};

use crate::entry::{Entry, Timestamp};
use crate::journal::Journal;
use crate::memtable::{BTreeTable, Memtable};
use crate::storage::{Storage, StorageVersion};
use crate::Result;

pub struct Database {
    core: Arc<Core>,
    last_ts: AtomicU64,
    write_tx: mpsc::Sender<PutEntry>,
    write_thread: thread::JoinHandle<()>,
}

impl Database {
    pub fn new<J, S>(journal: J, storage: S) -> Database
    where
        J: Journal + 'static,
        S: Storage + 'static,
    {
        let journal = Box::new(journal);
        let storage = Box::new(storage);
        let core = Arc::new(Core::new(journal, storage));
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
        let entry = Entry { ts, key, value };
        let put_entry = PutEntry { tx, entry };
        self.write_tx.send(put_entry).await?;
        rx.await?;
        Ok(())
    }
}

#[derive(Debug)]
struct PutEntry {
    tx: oneshot::Sender<()>,
    entry: Entry,
}

struct Core {
    mem: Arc<Box<dyn Memtable>>,
    current: RwLock<Arc<Version>>,
    journal: Box<dyn Journal>,
    storage: Box<dyn Storage>,
}

impl Core {
    fn new(journal: Box<dyn Journal>, storage: Box<dyn Storage>) -> Core {
        let mem: Box<dyn Memtable> = Box::new(BTreeTable::new());
        let mem = Arc::new(mem);
        let current = Arc::new(Version {
            mem: mem.clone(),
            imm: None,
            storage: storage.current(),
        });
        Core {
            mem,
            current: RwLock::new(current),
            journal,
            storage,
        }
    }

    async fn get(&self, ts: Timestamp, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let current = self.current.read().await.clone();
        current.get(ts, key).await
    }

    #[tokio::main]
    async fn handle_writes(&self, mut rx: mpsc::Receiver<PutEntry>) -> Result<()> {
        while let Some(put) = rx.recv().await {
            let data = put.entry.encode();
            self.journal.append(data).await?;
            put.tx.send(()).unwrap();
            self.mem.insert(put.entry);
        }
        Ok(())
    }
}

struct Version {
    mem: Arc<Box<dyn Memtable>>,
    imm: Option<Arc<Box<dyn Memtable>>>,
    storage: Arc<Box<dyn StorageVersion>>,
}

impl Version {
    async fn get(&self, ts: Timestamp, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if let Some(value) = self.mem.get(ts, key).await {
            return Ok(Some(value));
        }
        if let Some(imm) = &self.imm {
            if let Some(value) = imm.get(ts, key).await {
                return Ok(Some(value));
            }
        }
        self.storage.get(ts, key).await
    }
}
