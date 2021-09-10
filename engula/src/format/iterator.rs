use std::cmp::{Eq, Ord, Ordering, PartialEq, PartialOrd};

use async_trait::async_trait;

use super::Timestamp;
use crate::error::Result;

#[derive(Debug)]
pub struct Entry<'a>(pub Timestamp, pub &'a [u8], pub &'a [u8]);

impl<'a> From<(Timestamp, &'a [u8], &'a [u8])> for Entry<'a> {
    fn from(v: (Timestamp, &'a [u8], &'a [u8])) -> Entry {
        Entry(v.0, v.1, v.2)
    }
}

impl<'a> Eq for Entry<'a> {}

impl<'a> PartialEq for Entry<'a> {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0 && self.1 == other.1
    }
}

impl<'a> Ord for Entry<'a> {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.1.cmp(other.1) {
            Ordering::Greater => Ordering::Greater,
            Ordering::Less => Ordering::Less,
            Ordering::Equal => other.0.cmp(&self.0),
        }
    }
}

impl<'a> PartialOrd for Entry<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[async_trait]
pub trait Iterator: Send + Sync {
    async fn seek_to_first(&mut self);

    async fn seek(&mut self, ts: Timestamp, key: &[u8]);

    async fn next(&mut self);

    fn current(&self) -> Result<Option<Entry>>;
}

impl Eq for Box<dyn Iterator> {}

impl PartialEq for Box<dyn Iterator> {
    fn eq(&self, other: &Self) -> bool {
        match (self.current(), other.current()) {
            (Ok(left), Ok(right)) => match (left, right) {
                (Some(left), Some(right)) => left == right,
                (None, None) => true,
                _ => false,
            },
            _ => false,
        }
    }
}

impl Ord for Box<dyn Iterator> {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self.current(), other.current()) {
            (Ok(left), Ok(right)) => match (left, right) {
                (Some(left), Some(right)) => left.cmp(&right),
                (Some(_), None) => Ordering::Less,
                (None, Some(_)) => Ordering::Greater,
                (None, None) => Ordering::Equal,
            },
            (Err(_), _) => Ordering::Less,
            (_, Err(_)) => Ordering::Greater,
        }
    }
}

impl PartialOrd for Box<dyn Iterator> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
