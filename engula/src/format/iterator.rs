use std::cmp::{Eq, Ord, Ordering, PartialEq, PartialOrd};

use crate::common::Timestamp;

pub type Version<'a> = (Timestamp, &'a [u8], &'a [u8]);

pub trait Iterator {
    fn valid(&self) -> bool;

    fn seek_to_first(&mut self);

    fn seek(&mut self, ts: Timestamp, key: &[u8]);

    fn next(&mut self);

    fn current(&self) -> Option<Version>;
}

impl Eq for Box<dyn Iterator> {}

impl PartialEq for Box<dyn Iterator> {
    fn eq(&self, other: &Self) -> bool {
        match (self.current(), other.current()) {
            (Some(v1), Some(v2)) => (v1.0 == v2.0 && v1.1 == v2.1),
            (None, None) => true,
            _ => false,
        }
    }
}

impl Ord for Box<dyn Iterator> {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self.current(), other.current()) {
            (Some(v1), Some(v2)) => {
                if v1.1 == v2.1 {
                    Ordering::Equal
                } else {
                    if v1.0 < v2.0 {
                        Ordering::Greater
                    } else if v1.0 > v2.0 {
                        Ordering::Less
                    } else {
                        Ordering::Equal
                    }
                }
            }
            (Some(_), None) => Ordering::Less,
            (None, Some(_)) => Ordering::Greater,
            (None, None) => Ordering::Equal,
        }
    }
}

impl PartialOrd for Box<dyn Iterator> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
