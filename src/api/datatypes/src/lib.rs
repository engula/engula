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

mod hash_map;
mod int64;
mod list;

pub use hash_map::{HashMap, HashMapMutation, HashMapValue};
pub use int64::{Int64, Int64Mutation, Int64Value};
pub use list::{List, ListMutation, ListValue};

pub trait DataValue {
    type Value;
    type Mutation;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn example() {
        let mut im = Int64Mutation::default();
        im.add(1);
        let mut lm: ListMutation<Int64> = ListMutation::default();
        lm.append(1).update(2, im);
        let mut hm: HashMapMutation<i64, List<Int64>> = HashMapMutation::default();
        hm.insert(1, vec![1, 2, 3]).update(2, lm);
        // let co: HashMapCollection<i64, List<Int64>> =
        // HashMapCollection::new(); co.mutate(hm);
    }
}
