// Copyright 2021 The Engula Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

extern crate linked_hash_map;

use std::borrow::Borrow;
use std::collections::hash_map::RandomState;
use std::fmt;
use std::hash::{BuildHasher, Hash};

use linked_hash_map::LinkedHashMap;

pub struct LruCache<K: Eq + Hash, V , S: BuildHasher = RandomState> {
    map: LinkedHashMap<K, V, S>,
    max_size: usize,
}

impl <K: Eq + Hash, V>LruCache<K, V> {
    pub fn new(size: usize) -> Self {
        LruCache {
            map: LinkedHashMap::new(),
            max_size:size
        }
    }
}

impl  <K: Eq + Hash, V , S: BuildHasher> LruCache<K, V, S> {
    pub fn with_hasher(size: usize, hash_builder: S) -> Self{
        LruCache{
            map: LinkedHashMap::with_hasher(hash_builder),
            max_size: size,
        }
    }

    pub fn contains_key<Q: ?Sized>(&mut self, key: &Q) -> bool
        where K: Borrow<Q>,
              Q: Hash + Eq
    {
        self.get_mut(key).is_some()
    }

    pub fn insert(&mut self, k: K, v: V) -> Option<V>{
        let old_value = self.map.insert(k,v);
        if self.len() > self.size(){
            self.remove_lru();
        }
        old_value
    }

    pub fn get_mut<Q: ?Sized>(&mut self, key: &Q) -> Option<&mut V>
        where K: Borrow<Q>,
              Q: Hash + Eq
    {
        self.map.get_refresh(key)
    }

    pub fn remove<Q: ?Sized>(&mut self, k: &Q) -> Option<V>
        where K: Borrow<Q>,
              Q: Hash + Eq
    {
        self.map.remove(k)
    }

    pub fn size(&self) -> usize{
        self.max_size
    }

    pub fn set_size(&mut self, size: usize) {
        for _ in size..self.len() {
            self.remove_lru();
        }
        self.max_size = size;
    }

    pub fn remove_lru (&mut self) -> Option<(K,V)> {
        self.map.pop_front()
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool{
        self.map.is_empty()
    }

    pub fn clear(&mut self){
        self.map.clear()
    }

    pub fn iter(&self) -> Iter<K, V> { Iter(self.map.iter()) }

    pub fn iter_mut(&mut self) -> IterMut<K, V> { IterMut(self.map.iter_mut()) }
}

impl<K: Eq + Hash, V, S: BuildHasher> Extend<(K, V)> for LruCache<K, V, S> {
    fn extend<I: IntoIterator<Item=(K, V)>>(&mut self, iter: I) {
        for (k, v) in iter {
            self.insert(k, v);
        }
    }
}

impl<K: fmt::Debug + Eq + Hash, V: fmt::Debug, S: BuildHasher> fmt::Debug for LruCache<K, V, S> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_map().entries(self.iter().rev()).finish()
    }
}

impl<K: Eq + Hash, V, S: BuildHasher> IntoIterator for LruCache<K, V, S> {
    type Item = (K, V);
    type IntoIter = IntoIter<K, V>;

    fn into_iter(self) -> IntoIter<K, V> {
        IntoIter(self.map.into_iter())
    }
}

impl<'a, K: Eq + Hash, V, S: BuildHasher> IntoIterator for &'a LruCache<K, V, S> {
    type Item = (&'a K, &'a V);
    type IntoIter = Iter<'a, K, V>;
    fn into_iter(self) -> Iter<'a, K, V> { self.iter() }
}

impl<'a, K: Eq + Hash, V, S: BuildHasher> IntoIterator for &'a mut LruCache<K, V, S> {
    type Item = (&'a K, &'a mut V);
    type IntoIter = IterMut<'a, K, V>;
    fn into_iter(self) -> IterMut<'a, K, V> { self.iter_mut() }
}

#[derive(Clone)]
pub struct IntoIter<K, V>(linked_hash_map::IntoIter<K, V>);

impl<K, V> Iterator for IntoIter<K, V> {
    type Item = (K, V);

    fn next(&mut self) -> Option<(K, V)> {
        self.0.next()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.0.size_hint()
    }
}

impl<K, V> DoubleEndedIterator for IntoIter<K, V> {
    fn next_back(&mut self) -> Option<(K, V)> {
        self.0.next_back()
    }
}

impl<K, V> ExactSizeIterator for IntoIter<K, V> {
    fn len(&self) -> usize {
        self.0.len()
    }
}

pub struct Iter<'a, K: 'a, V: 'a>(linked_hash_map::Iter<'a, K, V>);

impl<'a, K, V> Clone for Iter<'a, K, V> {
    fn clone(&self) -> Iter<'a, K, V> { Iter(self.0.clone()) }
}

impl<'a, K, V> Iterator for Iter<'a, K, V> {
    type Item = (&'a K, &'a V);
    fn next(&mut self) -> Option<(&'a K, &'a V)> { self.0.next() }
    fn size_hint(&self) -> (usize, Option<usize>) { self.0.size_hint() }
}

impl<'a, K, V> DoubleEndedIterator for Iter<'a, K, V> {
    fn next_back(&mut self) -> Option<(&'a K, &'a V)> { self.0.next_back() }
}

impl<'a, K, V> ExactSizeIterator for Iter<'a, K, V> {
    fn len(&self) -> usize { self.0.len() }
}

pub struct IterMut<'a, K: 'a, V: 'a>(linked_hash_map::IterMut<'a, K, V>);

impl<'a, K, V> Iterator for IterMut<'a, K, V> {
    type Item = (&'a K, &'a mut V);
    fn next(&mut self) -> Option<(&'a K, &'a mut V)> { self.0.next() }
    fn size_hint(&self) -> (usize, Option<usize>) { self.0.size_hint() }
}

impl<'a, K, V> DoubleEndedIterator for IterMut<'a, K, V> {
    fn next_back(&mut self) -> Option<(&'a K, &'a mut V)> { self.0.next_back() }
}

impl<'a, K, V> ExactSizeIterator for IterMut<'a, K, V> {
    fn len(&self) -> usize { self.0.len() }
}

#[cfg(test)]
mod tests {
    use super::LruCache;

    #[test]
    fn test_put_and_get() {
        let mut cache = LruCache::new(2);
        cache.insert(1, 10);
        cache.insert(2, 20);
        assert_eq!(cache.get_mut(&1), Some(&mut 10));
        assert_eq!(cache.get_mut(&2), Some(&mut 20));
        assert_eq!(cache.len(), 2);
    }

    #[test]
    fn test_put_update() {
        let mut cache = LruCache::new(1);
        cache.insert("1", 10);
        cache.insert("1", 19);
        assert_eq!(cache.get_mut("1"), Some(&mut 19));
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn test_contains_key() {
        let mut cache = LruCache::new(1);
        cache.insert("1", 10);
        assert_eq!(cache.contains_key("1"), true);
    }

    #[test]
    fn test_expire_lru() {
        let mut cache = LruCache::new(2);
        cache.insert("foo1", "bar1");
        cache.insert("foo2", "bar2");
        cache.insert("foo3", "bar3");
        assert!(cache.get_mut("foo1").is_none());
        cache.insert("foo2", "bar2update");
        cache.insert("foo4", "bar4");
        assert!(cache.get_mut("foo3").is_none());
    }

    #[test]
    fn test_pop() {
        let mut cache = LruCache::new(2);
        cache.insert(1, 10);
        cache.insert(2, 20);
        assert_eq!(cache.len(), 2);
        let opt1 = cache.remove(&1);
        assert!(opt1.is_some());
        assert_eq!(opt1.unwrap(), 10);
        assert!(cache.get_mut(&1).is_none());
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn test_change_size() {
        let mut cache = LruCache::new(2);
        assert_eq!(cache.size(), 2);
        cache.insert(1, 10);
        cache.insert(2, 20);
        cache.set_size(1);
        assert!(cache.get_mut(&1).is_none());
        assert_eq!(cache.size(), 1);
    }

    #[test]
    fn test_debug() {
        let mut cache = LruCache::new(3);
        cache.insert(1, 10);
        cache.insert(2, 20);
        cache.insert(3, 30);
        assert_eq!(format!("{:?}", cache), "{3: 30, 2: 20, 1: 10}");
        cache.insert(2, 22);
        assert_eq!(format!("{:?}", cache), "{2: 22, 3: 30, 1: 10}");
        cache.insert(6, 60);
        assert_eq!(format!("{:?}", cache), "{6: 60, 2: 22, 3: 30}");
        cache.get_mut(&3);
        assert_eq!(format!("{:?}", cache), "{3: 30, 6: 60, 2: 22}");
        cache.set_size(2);
        assert_eq!(format!("{:?}", cache), "{3: 30, 6: 60}");
    }

    #[test]
    fn test_remove() {
        let mut cache = LruCache::new(3);
        cache.insert(1, 10);
        cache.insert(2, 20);
        cache.insert(3, 30);
        cache.insert(4, 40);
        cache.insert(5, 50);
        cache.remove(&3);
        cache.remove(&4);
        assert!(cache.get_mut(&3).is_none());
        assert!(cache.get_mut(&4).is_none());
        cache.insert(6, 60);
        cache.insert(7, 70);
        cache.insert(8, 80);
        assert!(cache.get_mut(&5).is_none());
        assert_eq!(cache.get_mut(&6), Some(&mut 60));
        assert_eq!(cache.get_mut(&7), Some(&mut 70));
        assert_eq!(cache.get_mut(&8), Some(&mut 80));
    }

    #[test]
    fn test_clear() {
        let mut cache = LruCache::new(2);
        cache.insert(1, 10);
        cache.insert(2, 20);
        cache.clear();
        assert!(cache.get_mut(&1).is_none());
        assert!(cache.get_mut(&2).is_none());
        assert_eq!(format!("{:?}", cache), "{}");
    }

    #[test]
    fn test_iter() {
        let mut cache = LruCache::new(3);
        cache.insert(1, 10);
        cache.insert(2, 20);
        cache.insert(3, 30);
        cache.insert(4, 40);
        cache.insert(5, 50);
        assert_eq!(cache.iter().collect::<Vec<_>>(),
                   [(&3, &30), (&4, &40), (&5, &50)]);
        assert_eq!(cache.iter_mut().collect::<Vec<_>>(),
                   [(&3, &mut 30), (&4, &mut 40), (&5, &mut 50)]);
        assert_eq!(cache.iter().rev().collect::<Vec<_>>(),
                   [(&5, &50), (&4, &40), (&3, &30)]);
        assert_eq!(cache.iter_mut().rev().collect::<Vec<_>>(),
                   [(&5, &mut 50), (&4, &mut 40), (&3, &mut 30)]);
    }
}