use std::cmp::{Eq, Ord, Ordering, PartialEq, PartialOrd};

use async_trait::async_trait;

use crate::common::Timestamp;
use crate::error::Error;

#[derive(Debug)]
pub struct Version<'a>(pub Timestamp, pub &'a [u8], pub &'a [u8]);

impl<'a> From<(Timestamp, &'a [u8], &'a [u8])> for Version<'a> {
    fn from(v: (Timestamp, &'a [u8], &'a [u8])) -> Version {
        Version(v.0, v.1, v.2)
    }
}

impl<'a> Eq for Version<'a> {}

impl<'a> PartialEq for Version<'a> {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0 && self.1 == other.1
    }
}

impl<'a> Ord for Version<'a> {
    fn cmp(&self, other: &Self) -> Ordering {
        let mut ord = self.1.cmp(&other.1);
        if ord == Ordering::Equal {
            if self.0 < other.0 {
                ord = Ordering::Greater;
            } else if self.0 > other.0 {
                ord = Ordering::Less;
            }
        }
        ord
    }
}

impl<'a> PartialOrd for Version<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[async_trait]
pub trait Iterator: Send + Sync {
    fn valid(&self) -> bool;

    fn error(&self) -> Option<Error>;

    async fn seek_to_first(&mut self);

    async fn seek(&mut self, ts: Timestamp, key: &[u8]);

    async fn next(&mut self);

    fn current(&self) -> Option<Version>;
}

impl Eq for Box<dyn Iterator> {}

impl PartialEq for Box<dyn Iterator> {
    fn eq(&self, other: &Self) -> bool {
        match (self.current(), other.current()) {
            (Some(left), Some(right)) => left == right,
            (None, None) => true,
            _ => false,
        }
    }
}

impl Ord for Box<dyn Iterator> {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self.current(), other.current()) {
            (Some(left), Some(right)) => left.cmp(&right),
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
