use async_trait::async_trait;

use super::{
    iterator::{Entry, Iterator},
    Timestamp,
};
use crate::error::{Error, Result};

#[async_trait]
pub trait IterGenerator: Sync + Send {
    async fn spawn(&self, index_value: &[u8]) -> Result<Box<dyn Iterator>>;
}

pub struct TwoLevelIterator {
    error: Option<Error>,
    index_iter: Box<dyn Iterator>,
    block_iter: Option<Box<dyn Iterator>>,
    block_iter_generator: Box<dyn IterGenerator>,
}

impl TwoLevelIterator {
    pub fn new(
        index_iter: Box<dyn Iterator>,
        block_iter_generator: Box<dyn IterGenerator>,
    ) -> TwoLevelIterator {
        TwoLevelIterator {
            error: None,
            index_iter,
            block_iter: None,
            block_iter_generator,
        }
    }

    async fn init_block_iter(&mut self) {
        loop {
            match self.index_iter.current() {
                Ok(Some(ent)) => match self.block_iter_generator.spawn(ent.2).await {
                    Ok(mut iter) => {
                        iter.seek_to_first().await;
                        match iter.current() {
                            Ok(Some(_)) | Err(_) => {
                                self.block_iter = Some(iter);
                                return;
                            }
                            _ => self.index_iter.next().await,
                        }
                    }
                    Err(err) => {
                        self.error = Some(err);
                        return;
                    }
                },
                Ok(None) => {
                    self.block_iter = None;
                    return;
                }
                Err(err) => {
                    self.error = Some(err);
                    return;
                }
            }
        }
    }
}

#[async_trait]
impl Iterator for TwoLevelIterator {
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
        if let Some(iter) = self.block_iter.as_mut() {
            iter.next().await;
            if let Ok(None) = iter.current() {
                self.index_iter.next().await;
                self.init_block_iter().await;
            }
        }
    }

    fn current(&self) -> Result<Option<Entry>> {
        if let Some(err) = self.error.as_ref() {
            return Err(err.clone());
        }
        if let Some(iter) = self.block_iter.as_ref() {
            iter.current()
        } else {
            Ok(None)
        }
    }
}
