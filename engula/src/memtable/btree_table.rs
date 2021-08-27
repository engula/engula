use std::collections::BTreeMap;
use std::ops::Bound::{Included, Unbounded};
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use tokio::sync::RwLock;

use super::memtable::Memtable;
use super::version::Version;
use crate::common::Timestamp;

type VersionedTree = BTreeMap<Version, Vec<u8>>;

pub struct BTreeTable {
    tree: RwLock<BTreeMap<Vec<u8>, VersionedTree>>,
    size: AtomicUsize,
}

impl BTreeTable {
    pub fn new() -> BTreeTable {
        BTreeTable {
            tree: RwLock::new(BTreeMap::new()),
            size: AtomicUsize::new(0),
        }
    }
}

#[async_trait]
impl Memtable for BTreeTable {
    async fn get(&self, ts: Timestamp, key: &[u8]) -> Option<Vec<u8>> {
        let tree = self.tree.read().await;
        tree.get(key).and_then(|x| {
            let mut range = x.range((Included(Version(ts)), Unbounded));
            range.next().map(|x| x.1.clone())
        })
    }

    async fn insert(&self, ts: Timestamp, key: Vec<u8>, value: Vec<u8>) {
        let mut tree = self.tree.write().await;
        tree.entry(key)
            .or_insert(VersionedTree::new())
            .insert(Version(ts), value);
    }

    fn approximate_size(&self) -> usize {
        self.size.load(Ordering::Relaxed)
    }
}
