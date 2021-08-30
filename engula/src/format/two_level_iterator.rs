use std::future::Future;
use std::pin::Pin;

use async_trait::async_trait;

use super::iterator::*;
use crate::common::Timestamp;
use crate::error::{Error, Result};

#[async_trait]
pub trait BlockIterGenerator: Sync + Send {
    async fn spawn(&self, index_value: &[u8]) -> Result<Box<dyn Iterator>>;
}

pub struct TwoLevelIterator {
    error: Option<Error>,
    index_iter: Box<dyn Iterator>,
    block_iter: Option<Box<dyn Iterator>>,
    block_iter_generator: Box<dyn BlockIterGenerator>,
}

impl TwoLevelIterator {
    pub fn new(
        index_iter: Box<dyn Iterator>,
        block_iter_generator: Box<dyn BlockIterGenerator>,
    ) -> TwoLevelIterator {
        TwoLevelIterator {
            error: None,
            index_iter,
            block_iter: None,
            block_iter_generator,
        }
    }

    async fn init_block_iter(&mut self) {
        while let Some(version) = self.index_iter.current() {
            match self.block_iter_generator.spawn(version.2).await {
                Ok(mut iter) => {
                    iter.seek_to_first().await;
                    if iter.valid() {
                        self.block_iter = Some(iter);
                        break;
                    }
                    if iter.error().is_some() {
                        self.block_iter = None;
                        break;
                    }
                    self.index_iter.next().await;
                }
                Err(error) => {
                    self.error = Some(error);
                    self.block_iter = None;
                    break;
                }
            }
        }
    }
}

#[async_trait]
impl Iterator for TwoLevelIterator {
    fn valid(&self) -> bool {
        self.error().is_none() && self.block_iter.as_ref().map_or(false, |x| x.valid())
    }

    fn error(&self) -> Option<Error> {
        self.error.clone().or(self
            .index_iter
            .error()
            .or(self.block_iter.as_ref().and_then(|x| x.error())))
    }

    async fn seek_to_first(&mut self) {
        self.index_iter.seek_to_first().await;
        self.init_block_iter().await;
    }

    async fn seek(&mut self, ts: Timestamp, target: &[u8]) {
        self.index_iter.seek(ts, target).await;
        self.init_block_iter().await;
        if let Some(iter) = self.block_iter.as_mut() {
            iter.seek(ts, target).await;
        }
    }

    async fn next(&mut self) {
        match self.block_iter.as_mut() {
            Some(iter) => {
                iter.next().await;
                if iter.valid() {
                    return;
                }
            }
            None => return,
        }
        self.index_iter.next().await;
        self.init_block_iter().await;
    }

    fn current(&self) -> Option<Version> {
        if self.valid() {
            self.block_iter.as_ref().and_then(|x| x.current())
        } else {
            None
        }
    }
}
