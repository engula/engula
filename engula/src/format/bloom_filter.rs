extern crate bit_vec;

use crate::format::{FilterBuilder, FilterReader};
use bit_vec::BitVec;
use std::collections::hash_map::{DefaultHasher, RandomState};
use std::hash::{BuildHasher, Hash, Hasher};

//  A  Bloom Filter implementation
//  About optimal k and m derived calculate we reference
//  https://sagi.io/bloom-filters-for-the-perplexed/#appendix
//  About use two hash functions to generate a sequence of hash values.
//  See analysis in [Kirsch,Mitzenmacher 2006].
//  https://www.eecs.harvard.edu/~michaelm/postscripts/rsa2008.pdf
pub struct BloomFilter {
    bitmap: BitVec,
    //size of the bit array
    m: u64,
    //number of hash functions
    k: u32,
    // double hash functions
    hashers: [DefaultHasher; 2],
}

impl BloomFilter {
    #[allow(dead_code)]
    // count : number of elements to insert into bloom filter
    // false_positive : Bloom filter false_positive rate
    pub fn new(count: usize, false_positive: f64) -> BloomFilter {
        let m = m_size(count, false_positive);
        let k = k_size(false_positive);
        let hashers = [
            RandomState::new().build_hasher(),
            RandomState::new().build_hasher(),
        ];
        BloomFilter {
            bitmap: BitVec::from_elem(m, false),
            m: m as u64,
            k,
            hashers,
        }
    }

    pub fn insert(&mut self, key: &[u8]) {
        let (h1, h2) = self.kernel(key);
        for i in 0..self.k {
            let index = self.get_index(h1, h2, i as u64);
            self.bitmap.set(index, true);
        }
    }

    // There can be false positives, but no false negatives.
    pub fn contains(&self, key: &[u8]) -> bool {
        let (h1, h2) = self.kernel(key);
        for i in 0..self.k {
            let index = self.get_index(h1, h2, i as u64);
            if !self.bitmap.get(index).unwrap() {
                return false;
            }
        }
        true
    }

    // Calculate two hash values from  k hashes.
    fn kernel(&self, key: &[u8]) -> (u64, u64) {
        let hasher_1 = &mut self.hashers[0].clone();
        let hasher_2 = &mut self.hashers[1].clone();

        key.hash(hasher_1);
        key.hash(hasher_2);

        let hash_1 = hasher_1.finish();
        let hash_2 = hasher_2.finish();

        (hash_1, hash_2)
    }

    // Get the index from hash value of `k_i`.
    fn get_index(&self, h1: u64, h2: u64, i: u64) -> usize {
        (h1.wrapping_add((i).wrapping_mul(h2)) % self.m) as usize
    }
}

// Calculate the size of  bit array
// m = -( count * ln(false_positive) / (ln2)^2 )
fn m_size(count: usize, false_positive: f64) -> usize {
    let ln2_square = core::f64::consts::LN_2 * core::f64::consts::LN_2;
    ((-1.0f64 * count as f64 * false_positive.ln()) / ln2_square).ceil() as usize
}

//  Calculate the number of hash functions
//  k = (m / cnout) * ln2
//  so k = -(ln(false_positive) / ln2)
fn k_size(false_positive: f64) -> u32 {
    ((-1.0f64 * false_positive.ln()) / core::f64::consts::LN_2).ceil() as u32
}

impl FilterReader for BloomFilter {
    fn may_exist(&self, key: &[u8]) -> bool {
        self.contains(key)
    }
}

impl FilterBuilder for BloomFilter {
    fn add(&mut self, key: &[u8]) {
        self.insert(key);
    }

    fn finish(&mut self) -> Vec<u8> {
        self.bitmap.to_bytes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn insert() {
        let mut test = BloomFilter::new(100, 0.01);
        test.insert("item".as_bytes());
        assert!(test.contains("item".as_bytes()));
    }
    #[test]
    fn check_and_insert() {
        let mut test = BloomFilter::new(100, 0.01);
        assert!(!test.contains("item".as_bytes()));
        assert!(!test.contains("item_2".as_bytes()));
        test.insert("item_1".as_bytes());
        assert!(test.contains("item_1".as_bytes()));
    }
}
