use std::{collections::hash_map::DefaultHasher, hash::Hasher, sync::Arc};

use async_trait::async_trait;
use tokio::sync::Mutex;

use super::Cache;

pub struct LruCache {
    shards: Vec<Mutex<LruShard>>,
}

impl LruCache {
    pub fn new(capacity: usize, num_shards: usize) -> LruCache {
        let mut shards = Vec::new();
        for _ in 0..num_shards {
            let shard = LruShard::new(capacity / num_shards);
            shards.push(Mutex::new(shard));
        }
        LruCache { shards }
    }

    fn index(&self, key: &[u8]) -> usize {
        let mut hasher = DefaultHasher::new();
        hasher.write(key);
        hasher.finish() as usize % self.shards.len()
    }
}

#[async_trait]
impl Cache for LruCache {
    async fn get(&self, key: &[u8]) -> Option<Arc<Vec<u8>>> {
        let mut shard = self.shards[self.index(key)].lock().await;
        shard.get(key).await
    }

    async fn put(&self, key: Vec<u8>, value: Arc<Vec<u8>>) {
        let mut shard = self.shards[self.index(&key)].lock().await;
        shard.put(key, value).await
    }
}

struct LruShard {
    cache: lru::LruCache<Vec<u8>, Arc<Vec<u8>>>,
    capacity: usize,
    used_size: usize,
}

impl LruShard {
    fn new(capacity: usize) -> LruShard {
        LruShard {
            cache: lru::LruCache::unbounded(),
            capacity,
            used_size: 0,
        }
    }

    async fn get(&mut self, key: &[u8]) -> Option<Arc<Vec<u8>>> {
        self.cache.get(key).cloned()
    }

    async fn put(&mut self, key: Vec<u8>, value: Arc<Vec<u8>>) {
        let size = key.len() + value.len();
        while self.used_size + size >= self.capacity {
            if let Some(e) = self.cache.pop_lru() {
                self.used_size -= e.0.len() + e.1.len();
            } else {
                return;
            }
        }
        self.used_size += size;
        self.cache.put(key, value);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test() {
        let cache = LruCache::new(8, 1);
        let k1 = vec![0];
        let v1 = Arc::new(vec![1]);
        cache.put(k1.clone(), v1.clone()).await;
        assert_eq!(cache.get(&k1).await, Some(v1.clone()));
        let k2 = vec![2];
        let v2 = Arc::new(vec![3, 4]);
        cache.put(k2.clone(), v2.clone()).await;
        assert_eq!(cache.get(&k2).await, Some(v2.clone()));
        let k3 = vec![5];
        let v3 = Arc::new(vec![6, 7, 8]);
        cache.put(k3.clone(), v3.clone()).await;
        assert_eq!(cache.get(&k3).await, Some(v3.clone()));

        assert_eq!(cache.get(&k1).await, None);
        assert_eq!(cache.get(&k2).await, Some(v2.clone()));

        cache.put(k1.clone(), v1.clone()).await;
        assert_eq!(cache.get(&k1).await, Some(v1));
        assert_eq!(cache.get(&k2).await, Some(v2.clone()));
        assert_eq!(cache.get(&k3).await, None);
    }
}
