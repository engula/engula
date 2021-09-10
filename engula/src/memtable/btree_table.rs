use std::{
    cmp::Reverse,
    collections::btree_map,
    collections::BTreeMap,
    ops::Bound::{Included, Unbounded},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use async_trait::async_trait;
use tokio::sync::{OwnedRwLockReadGuard, RwLock};

use crate::{
    format::Timestamp,
    memtable::{MemItem, MemIter, MemSnapshot, MemTable},
};

type Sequence = Reverse<Timestamp>;
type SequenceTree = BTreeMap<Sequence, Vec<u8>>;

pub struct BTreeTable {
    tree: Arc<RwLock<BTreeMap<Vec<u8>, SequenceTree>>>,
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
            let mut range = x.range((Included(Reverse(ts)), Unbounded));
            range.next().map(|x| x.1.clone())
        })
    }

    async fn put(&self, ts: Timestamp, key: Vec<u8>, value: Vec<u8>) {
        let size = std::mem::size_of_val(&ts) + key.len() + value.len();
        self.size.fetch_add(size, Ordering::Relaxed);
        let mut tree = self.tree.write().await;
        tree.entry(key)
            .or_insert_with(SequenceTree::new)
            .insert(Reverse(ts), value);
    }

    async fn snapshot(&self) -> Box<dyn MemSnapshot> {
        let tree = self.tree.clone().read_owned().await;
        Box::new(BTreeSnapshot(tree))
    }

    fn approximate_size(&self) -> usize {
        self.size.load(Ordering::Relaxed)
    }
}

type InnerIter<'a> = Option<(&'a [u8], btree_map::Iter<'a, Sequence, Vec<u8>>)>;

struct BTreeIter<'a> {
    outer_iter: btree_map::Iter<'a, Vec<u8>, SequenceTree>,
    inner_iter: InnerIter<'a>,
}

impl<'a> BTreeIter<'a> {
    fn new(iter: btree_map::Iter<'a, Vec<u8>, SequenceTree>) -> BTreeIter<'a> {
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
                    return Some((pair.0 .0, iter.0, pair.1));
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

struct BTreeSnapshot(OwnedRwLockReadGuard<BTreeMap<Vec<u8>, SequenceTree>>);

impl MemSnapshot for BTreeSnapshot {
    fn iter(&self) -> Box<MemIter> {
        Box::new(BTreeIter::new(self.0.iter()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test() {
        let mem = BTreeTable::new();
        mem.put(1, vec![1], vec![1]).await;
        assert_eq!(mem.get(1, &[1]).await, Some(vec![1]));
        mem.put(2, vec![2], vec![2]).await;
        assert_eq!(mem.get(2, &[1]).await, Some(vec![1]));
        mem.put(3, vec![1], vec![3]).await;
        mem.put(4, vec![2], vec![4]).await;
        mem.put(5, vec![5], vec![5]).await;
        assert_eq!(mem.get(3, &[1]).await, Some(vec![3]));
        assert_eq!(mem.get(4, &[2]).await, Some(vec![4]));
        let snap = mem.snapshot().await;
        let mut iter = snap.iter();
        assert_eq!(iter.next(), Some((3, [1u8].as_ref(), [3u8].as_ref())));
        assert_eq!(iter.next(), Some((1, [1u8].as_ref(), [1u8].as_ref())));
        assert_eq!(iter.next(), Some((4, [2u8].as_ref(), [4u8].as_ref())));
        assert_eq!(iter.next(), Some((2, [2u8].as_ref(), [2u8].as_ref())));
        assert_eq!(iter.next(), Some((5, [5u8].as_ref(), [5u8].as_ref())));
        assert_eq!(iter.next(), None);
    }
}
