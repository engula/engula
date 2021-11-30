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

use super::Measure_trait;

// size limit based on a count of cache.
pub struct Count;

impl<K, V> Measure_trait<K, V> for Count {
    type Measure = ();

    fn measure<Q: ?Sized>(&self, key: &Q, value: &V) -> Self::Measure
    where
        K: Borrow<Q>,
    {
    }
}

pub trait Countable<K, V>: Measure_trait<K, V> {
    fn add(&self, current: Self::Measure, amount: Self::Measure) -> Self::Measure;

    fn sub(&self, current: Self::Measure, amount: Self::Measure) -> Self::Measure;

    fn size(&self, current: Self::Measure) -> Option<usize>;
}

pub trait CountableWithMeasure<K, V, M> {
    fn mea_add(&self, current: M, amount: M) -> M;

    fn mea_sub(&self, current: M, amount: M) -> M;

    fn mea_size(&self, current: M) -> Option<usize>;
}

impl<K, V, T: Measure_trait<K, V>> Countable<K, V> for T
where
    T: CountableWithMeasure<K, V, <T as Measure_trait<K, V>>::Measure>,
{
    // Add `amount` to `current` and return the sum.
    fn add(&self, current: Self::Measure, amount: Self::Measure) -> Self::Measure {
        CountableWithMeasure::mea_add(self, current, amount)
    }

    // Subtract `amount` from `current` and return the difference.
    fn sub(&self, current: Self::Measure, amount: Self::Measure) -> Self::Measure {
        CountableWithMeasure::mea_sub(self, current, amount)
    }

    // Return `current` as a `usize` if possible, otherwise return `None`.
    //
    // If this method returns `None` the cache will use the number of cache entries
    // as its size.
    fn size(&self, current: Self::Measure) -> Option<usize> {
        CountableWithMeasure::mea_size(self, current)
    }
}

// `Count` is all no-ops, the number of entries in the map is the size.
impl<K, V, T> CountableWithMeasure<K, V, usize> for T
where
    T: Measure_trait<K, V>,
{
    // Add `amount` to `current` and return the sum.
    fn mea_add(&self, current: usize, amount: usize) -> usize {
        current + amount
    }

    // Subtract `amount` from `current` and return the difference.
    fn mea_sub(&self, current: usize, amount: usize) -> usize {
        current - amount
    }

    // Return `current` as a `usize` if possible, otherwise return `None`.
    //
    // If this method returns `None` the cache will use the number of cache entries
    // as its size.
    fn mea_size(&self, current: usize) -> Option<usize> {
        Some(current)
    }
}

// For any other `Meter` with `Measure=usize`, just do the simple math.
impl<K, V> CountableMeterWithMeasure<K, V, ()> for Count {
    fn mea_add(&self, _current: (), _amount: ()) {}

    fn mea_sub(&self, _current: (), _amount: ()) {}

    fn mea_size(&self, _current: ()) -> Option<usize> {
        None
    }
}
