use super::iterator::*;
use crate::common::Timestamp;

pub struct TwoLevelIterator {
    index_iter: Box<dyn Iterator>,
    block_func: Box<dyn Fn(&[u8]) -> Box<dyn Iterator>>,
    second_iter: Option<Box<dyn Iterator>>,
}

impl TwoLevelIterator {
    fn new(
        index_iter: Box<dyn Iterator>,
        block_func: Box<dyn Fn(&[u8]) -> Box<dyn Iterator>>,
    ) -> TwoLevelIterator {
        TwoLevelIterator {
            index_iter,
            block_func,
            second_iter: None,
        }
    }

    fn init_second_iter(&mut self) {
        while let Some(version) = self.index_iter.current() {
            let mut iter = (*self.block_func)(version.2);
            iter.seek_to_first();
            if iter.valid() {
                self.second_iter = Some(iter);
                break;
            }
            self.index_iter.next();
        }
    }
}

impl Iterator for TwoLevelIterator {
    fn valid(&self) -> bool {
        self.second_iter.as_ref().map_or(false, |x| x.valid())
    }

    fn seek_to_first(&mut self) {
        self.index_iter.seek_to_first();
        self.init_second_iter();
    }

    fn seek(&mut self, ts: Timestamp, target: &[u8]) {
        self.index_iter.seek(ts, target);
        self.init_second_iter();
        self.second_iter.as_mut().map(|x| x.seek(ts, target));
    }

    fn next(&mut self) {
        match self.second_iter.as_mut() {
            Some(iter) => {
                iter.next();
                if iter.valid() {
                    return;
                }
            }
            None => return,
        }
        self.index_iter.next();
        self.init_second_iter();
    }

    fn current(&self) -> Option<Version> {
        self.second_iter.as_ref().and_then(|x| x.current())
    }
}
