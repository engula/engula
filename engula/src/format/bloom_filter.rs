use crate::filter::{FilterBuilder, FilterReader};

use bit_vec::BitVec;
use std::collections::hash_map::DefaultHasher;
use std::hash::Hasher;

//  A  Bloom Filter implementation
//  About optimal k and m derived calculate we reference
//  https://sagi.io/bloom-filters-for-the-perplexed/#appendix
//  About use two hash functions to generate a sequence of hash values.
//  See analysis in [Kirsch,Mitzenmacher 2006].
//  https://www.eecs.harvard.edu/~michaelm/postscripts/rsa2008.pdf
pub struct BloomFilter {
    // bit array
    bitmap: BitVec,
    //size of the bit array
    m: u32,
    //number of hash functions
    k: u32,
    //size of BloomFilter
    count: usize,
    //num of elements
    num_elements: usize,
    // status of bloomfilter 1: full 0: not full
    full: u8,
}

impl BloomFilter {
    #[allow(dead_code)]
    pub fn new(count: usize, false_positive: f64) -> BloomFilter {
        let m = m_size(count, false_positive);
        let k = k_size(false_positive);
        BloomFilter {
            bitmap: BitVec::from_elem(m as usize, false),
            m: m as u32,
            k,
            count,
            num_elements: 0,
            full: 0,
        }
    }

    pub fn insert(&mut self, key: &[u8]) {
        if self.num_elements == self.count {
            self.full = 1;
            return;
        }
        let (h1, h2) = self.kernel(key);
        for i in 0..self.k {
            let index = self.get_index(h1, h2, i);
            self.bitmap.set(index, true);
        }
        self.num_elements += 1;
    }

    fn kernel(&self, key: &[u8]) -> (u32, u32) {
        let mut hasher = DefaultHasher::new();
        hasher.write(key);
        // (hash & (2_u128.pow(64) - 1)) as u64;
        let h1 = (hasher.finish() & (2_u64.pow(32) - 1)) as u32;
        let h2 = (h1 << 17) | (h1 >> 15) as u32;
        (h1, h2)
    }

    // There can be false positives, but no false negatives.
    pub fn contains(&self, key: &[u8]) -> bool {
        let (h1, h2) = self.kernel(key);
        for i in 0..self.k {
            let index = self.get_index(h1, h2, i);
            if !self.bitmap.get(index).unwrap() {
                return false;
            }
        }
        true
    }

    // Get the index from hash value of `k_i`.
    fn get_index(&self, h1: u32, h2: u32, i: u32) -> usize {
        (h1.wrapping_add((i).wrapping_mul(h2)) % self.m) as usize
    }
}

// Calculate the size of  bit array
// m = -( count * ln(false_positive) / (ln2)^2 )
fn m_size(count: usize, false_positive: f64) -> u32 {
    let ln2_square = core::f64::consts::LN_2 * core::f64::consts::LN_2;
    ((-1.0f64 * count as f64 * false_positive.ln()) / ln2_square).ceil() as u32
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
        if self.full == 1 {
            let full = self.full;
            vec![full]
        } else {
            self.bitmap.to_bytes()
        }
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
    #[test]
    fn full() {
        let mut test = BloomFilter::new(10000, 0.01);
        let mut hasher = DefaultHasher::new();
        for i in 0..10002 {
            hasher.write_i32(i);
            let h = hasher.finish();
            test.insert(h.to_be_bytes().to_vec().as_slice());
            if test.full == 1 {
                assert!(!test.contains(h.to_be_bytes().to_vec().as_slice()));
            } else {
                assert!(test.contains(h.to_be_bytes().to_vec().as_slice()));
            }
        }
    }
}
