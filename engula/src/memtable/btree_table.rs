use std::collections::btree_map;
use std::collections::BTreeMap;
use std::ops::Bound::{Included, Unbounded};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::{OwnedRwLockReadGuard, RwLock};

use super::memtable::{MemItem, MemIter, MemSnapshot, MemTable};
use super::version::Version;
use crate::common::Timestamp;

type VersionTree = BTreeMap<Version, Vec<u8>>;

pub struct BTreeTable {
    tree: Arc<RwLock<BTreeMap<Vec<u8>, VersionTree>>>,
    size: AtomicUsize,
}

impl BTreeTable {
    pub fn new() -> BTreeTable {
        BTreeTable {
            tree: Arc::new(RwLock::new(BTreeMap::new())),
            size: AtomicUsize::new(0),
        }
    }
}

#[async_trait]
impl MemTable for BTreeTable {
    async fn get(&self, ts: Timestamp, key: &[u8]) -> Option<Vec<u8>> {
        let tree = self.tree.read().await;
        tree.get(key).and_then(|x| {
            let mut range = x.range((Included(Version(ts)), Unbounded));
            range.next().map(|x| x.1.clone())
        })
    }

    async fn put(&self, ts: Timestamp, key: Vec<u8>, value: Vec<u8>) {
        let size = std::mem::size_of_val(&ts) + key.len() + value.len();
        self.size.fetch_add(size, Ordering::Relaxed);
        let mut tree = self.tree.write().await;
        tree.entry(key)
            .or_insert(VersionTree::new())
            .insert(Version(ts), value);
    }

    async fn snapshot(&self) -> Box<dyn MemSnapshot> {
        let tree = self.tree.clone().read_owned().await;
        Box::new(BTreeSnapshot(tree))
    }

    fn approximate_size(&self) -> usize {
        self.size.load(Ordering::Relaxed)
    }
}

struct BTreeIter<'a> {
    outer_iter: btree_map::Iter<'a, Vec<u8>, VersionTree>,
    inner_iter: Option<(&'a [u8], btree_map::Iter<'a, Version, Vec<u8>>)>,
}

impl<'a> BTreeIter<'a> {
    fn new(iter: btree_map::Iter<'a, Vec<u8>, VersionTree>) -> BTreeIter<'a> {
        BTreeIter {
            outer_iter: iter,
            inner_iter: None,
        }
    }
}

impl<'a> Iterator for BTreeIter<'a> {
    type Item = MemItem<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(iter) = &mut self.inner_iter {
                if let Some(pair) = iter.1.next() {
                    return Some((pair.0 .0, &iter.0, pair.1));
                }
            }
            if let Some(pair) = self.outer_iter.next() {
                self.inner_iter = Some((pair.0, pair.1.iter()));
            } else {
                return None;
            }
        }
    }
}

struct BTreeSnapshot(OwnedRwLockReadGuard<BTreeMap<Vec<u8>, VersionTree>>);

impl MemSnapshot for BTreeSnapshot {
    fn iter(&self) -> Box<MemIter> {
        Box::new(BTreeIter::new(self.0.iter()))
    }
}
