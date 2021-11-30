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

use std::borrow::Borrow;

use heap_size::HeapSizeOf;

use super::Measure_trait;

// Size limit based on the heap size of each cache item.
//
// Requires cache entries that implement [`HeapSizeOf`][1].

pub struct HeapSize;

impl<K, V: HeapSizeOf> Measure_trait<K, V> for HeapSize {
    type Measure = usize;

    fn measure<Q: ?Sized>(&self, _: &Q, value: &V) -> Self::Measure
    where
        K: Borrow<Q>,
    {
        value.heap_size_of_children() + ::std::mem::size_of::<V>()
    }
}
