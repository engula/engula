// Copyright 2022 The Engula Authors.
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

use std::hash::Hash;

use hashbrown::raw::RawTable;

use super::{ObjectLayout, ObjectType};
use crate::elements::{entry::Entry, BoxElement};

pub trait HashKey {
    fn key(&self) -> &[u8];
}

impl HashKey for BoxElement<Entry> {
    fn key(&self) -> &[u8] {
        self.data_slice().0
    }
}

#[repr(C)]
pub struct RawHashMap<T: HashKey> {
    current: RawTable<T>,
}

impl<T: HashKey> RawHashMap<T> {
    pub fn get(&self, key: &[u8]) -> Option<&T> {
        match self.current.get(make_hash(key), equivalent_key(key)) {
            Some(entry) => Some(&*entry),
            None => None,
        }
    }

    pub fn get_mut(&mut self, key: &[u8]) -> Option<&mut T> {
        match self.current.get_mut(make_hash(key), equivalent_key(key)) {
            Some(entry) => Some(&mut *entry),
            None => None,
        }
    }

    pub fn insert(&mut self, entry: T) -> Option<T> {
        let key = entry.key();
        let code = make_hash(key);

        if let Some(old_entry) = self.current.get_mut(code, equivalent_key(key)) {
            Some(std::mem::replace(old_entry, entry))
        } else {
            self.current.insert(code, entry, make_entry_hash);
            None
        }
    }
}

impl<T: HashKey> Default for RawHashMap<T> {
    fn default() -> Self {
        RawHashMap {
            current: RawTable::default(),
        }
    }
}

pub type HashMap = RawHashMap<BoxElement<Entry>>;

impl ObjectLayout for HashMap {
    fn object_type() -> u16 {
        ObjectType::HASH_TABLE.bits
    }
}

fn make_hash<K>(val: &K) -> u64
where
    K: Hash + ?Sized,
{
    use core::hash::Hasher;
    use std::collections::hash_map::DefaultHasher;
    let mut state = DefaultHasher::new();
    val.hash(&mut state);
    state.finish()
}

fn equivalent_key<T: HashKey>(k: &[u8]) -> impl Fn(&T) -> bool + '_ {
    move |x| k.eq(x.key())
}

fn make_entry_hash<T: HashKey>(entry: &T) -> u64 {
    use core::hash::Hasher;
    use std::collections::hash_map::DefaultHasher;
    let mut state = DefaultHasher::new();
    entry.key().hash(&mut state);
    state.finish()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::objects::BoxObject;

    #[test]
    fn hash_map() {
        let mut hash_map: BoxObject<HashMap> = BoxObject::with_key(&[1, 2, 3]);

        let key = vec![0u8, 1u8];
        let key = &key;

        // 1. get not found
        assert!(hash_map.get(key).is_none());

        // 2. insert
        let mut entry = BoxElement::<Entry>::with_capacity(2, 5);
        let (key_buf, val_buf) = entry.data_slice_mut();
        key_buf.copy_from_slice(key);
        val_buf.copy_from_slice(&[0u8, 1, 2, 3, 4]);
        let res = hash_map.insert(entry);
        assert!(res.is_none());

        // 3. get found
        assert!(hash_map.get(key).is_some());

        // 4. overwrite and got old value
        let mut entry = BoxElement::<Entry>::with_capacity(2, 5);
        let (key_buf, val_buf) = entry.data_slice_mut();
        key_buf.copy_from_slice(key);
        val_buf.copy_from_slice(&[2u8, 3, 4, 5, 6]);
        let entry = hash_map.insert(entry).unwrap();
        let (_, val_buf) = entry.data_slice();
        assert_eq!(val_buf, &[0, 1, 2, 3, 4]);
    }
}
