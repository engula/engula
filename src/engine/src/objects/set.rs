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

use super::{
    hash::{HashKey, RawHashMap},
    ObjectLayout, ObjectType,
};
use crate::elements::{array::Array, BoxElement};

impl HashKey for BoxElement<Array> {
    fn key(&self) -> &[u8] {
        self.data_slice()
    }
}

pub type HashSet = RawHashMap<BoxElement<Array>>;

impl ObjectLayout for HashSet {
    fn object_type() -> u16 {
        ObjectType::SET.bits
    }
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
