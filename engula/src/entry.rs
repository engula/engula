use std::cmp::{Ord, Ordering};

pub type Timestamp = u64;

#[derive(Eq, Debug)]
pub struct Entry {
    pub ts: Timestamp,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

impl Entry {
    pub fn encode(&self) -> Vec<u8> {
        Vec::new()
    }
}

impl Ord for Entry {
    fn cmp(&self, other: &Self) -> Ordering {
        let mut ord = self.key.cmp(&other.key);
        if ord == Ordering::Equal {
            if self.ts < other.ts {
                ord = Ordering::Less
            } else if self.ts > other.ts {
                ord = Ordering::Greater
            }
        }
        ord
    }
}

impl PartialOrd for Entry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for Entry {
    fn eq(&self, other: &Self) -> bool {
        self.ts == other.ts && self.key == other.key
    }
}
