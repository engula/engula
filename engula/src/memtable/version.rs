use std::cmp::{Ord, Ordering, PartialOrd};

use crate::common::Timestamp;

#[derive(Eq, PartialEq)]
pub struct Version(pub Timestamp);

impl Ord for Version {
    fn cmp(&self, other: &Self) -> Ordering {
        if self.0 > other.0 {
            Ordering::Less
        } else if self.0 < other.0 {
            Ordering::Greater
        } else {
            Ordering::Equal
        }
    }
}

impl PartialOrd for Version {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
