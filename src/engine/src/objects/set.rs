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
use crate::elements::{array::Array, BoxElement, Element};

#[repr(C)]
#[derive(Default)]
pub struct HashSet {
    current: RawTable<BoxElement<Array>>,
}

impl HashSet {
    pub fn get(&self, key: &[u8]) -> Option<&Element<Array>> {
        match self.current.get(make_hash(key), equivalent_key(key)) {
            Some(entry) => Some(&*entry),
            None => None,
        }
    }

    pub fn get_mut(&mut self, key: &[u8]) -> Option<&mut Element<Array>> {
        match self.current.get_mut(make_hash(key), equivalent_key(key)) {
            Some(entry) => Some(&mut *entry),
            None => None,
        }
    }

    pub fn insert(&mut self, entry: BoxElement<Array>) -> Option<BoxElement<Array>> {
        let key = entry.data_slice();
        let code = make_hash(key);

        if let Some(old_entry) = self.current.get_mut(code, equivalent_key(key)) {
            Some(std::mem::replace(old_entry, entry))
        } else {
            self.current.insert(code, entry, make_entry_hash);
            None
        }
    }
}

impl ObjectLayout for HashSet {
    fn object_type() -> u16 {
        ObjectType::SET.bits
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

fn equivalent_key(k: &[u8]) -> impl Fn(&BoxElement<Array>) -> bool + '_ {
    move |x| k.eq(x.data_slice())
}

fn make_entry_hash(entry: &BoxElement<Array>) -> u64 {
    use core::hash::Hasher;
    use std::collections::hash_map::DefaultHasher;
    let mut state = DefaultHasher::new();
    entry.data_slice().hash(&mut state);
    state.finish()
}

#[cfg(test)]

mod tests {
    use super::*;
    use crate::objects::BoxObject;

    #[test]
    fn hash_set() {
        let mut hash_set: BoxObject<HashSet> = BoxObject::with_key(&[1, 2, 3]);

        let key = vec![0u8, 1u8, 2, 3, 4];
        let key = &key;

        // 1. get not found
        assert!(hash_set.get(key).is_none());

        // 2. insert
        let mut entry = BoxElement::<Array>::with_capacity(5);
        let key_buf = entry.data_slice_mut();
        key_buf.copy_from_slice(key);
        let res = hash_set.insert(entry);
        assert!(res.is_none());

        // 3. get found
        assert!(hash_set.get(key).is_some());

        // 4. overwrite and got old value
        let mut entry = BoxElement::<Array>::with_capacity(5);
        let key_buf = entry.data_slice_mut();
        key_buf.copy_from_slice(key);
        let entry = hash_set.insert(entry).unwrap();
        let val_buf = entry.data_slice();
        assert_eq!(val_buf, &[0, 1, 2, 3, 4]);
    }
}
