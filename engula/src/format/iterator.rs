use std::cmp::{Eq, Ord, Ordering, PartialEq, PartialOrd};

use async_trait::async_trait;

use crate::common::Timestamp;
use crate::error::Error;

pub type Version<'a> = (Timestamp, &'a [u8], &'a [u8]);

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
            (Some(left), Some(right)) => (left.0 == right.0 && left.1 == right.1),
            (None, None) => true,
            _ => false,
        }
    }
}

impl Ord for Box<dyn Iterator> {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self.current(), other.current()) {
            (Some(left), Some(right)) => {
                let mut ord = left.1.cmp(&right.1);
                if ord == Ordering::Equal {
                    if left.0 < right.0 {
                        ord = Ordering::Greater;
                    } else if left.0 > right.0 {
                        ord = Ordering::Less;
                    }
                }
                ord
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
