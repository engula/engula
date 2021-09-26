use std::sync::Arc;

use async_trait::async_trait;

use super::{iterator::Iterator, TableDesc, Timestamp};
use crate::{cache::Cache, error::Result};

#[derive(Clone)]
pub struct TableReaderOptions {
    pub cache: Option<Arc<dyn Cache>>,
    pub prefetch: bool,
}

impl TableReaderOptions {
    pub fn default() -> TableReaderOptions {
        TableReaderOptions {
            cache: None,
            prefetch: false,
        }
    }
}

#[async_trait]
pub trait TableReader: Send + Sync {
    async fn get(&self, ts: Timestamp, key: &[u8]) -> Result<Option<Vec<u8>>>;

    fn new_iterator(&self) -> Box<dyn Iterator>;
}

#[async_trait]
pub trait TableBuilder: Send {
    async fn add(&mut self, ts: Timestamp, key: &[u8], value: &[u8]);

    async fn finish(&mut self) -> Result<TableDesc>;
}
