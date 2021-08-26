use std::collections::BTreeSet;
use std::ops::Bound::{Included, Unbounded};

use async_trait::async_trait;
use tokio::sync::RwLock;

use crate::entry::{Entry, Timestamp};

#[async_trait]
pub trait Memtable: Send + Sync {
    async fn get(&self, ts: Timestamp, key: &[u8]) -> Option<Vec<u8>>;

    async fn insert(&self, entry: Entry);
}

pub struct BTreeTable {
    table: RwLock<BTreeSet<Entry>>,
}

impl BTreeTable {
    pub fn new() -> BTreeTable {
        BTreeTable {
            table: RwLock::new(BTreeSet::new()),
        }
    }
}

#[async_trait]
impl Memtable for BTreeTable {
    async fn get(&self, ts: Timestamp, key: &[u8]) -> Option<Vec<u8>> {
        let entry = Entry {
            ts,
            key: key.to_owned(),
            value: vec![],
        };
        let table = self.table.read().await;
        let mut range = table.range((Included(&entry), Unbounded));
        range.next().map(|x| x.value.clone())
    }

    async fn insert(&self, entry: Entry) {
        let mut table = self.table.write().await;
        table.insert(entry);
    }
}
