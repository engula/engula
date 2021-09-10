mod sst_storage;

pub use sst_storage::SstStorage;

use async_trait::async_trait;

use crate::{
    error::Result,
    format::{TableBuilder, TableDesc, TableReader},
};

#[async_trait]
pub trait Storage: Send + Sync {
    async fn new_reader(&self, desc: &TableDesc) -> Result<Box<dyn TableReader>>;

    async fn new_builder(&self, table_number: u64) -> Result<Box<dyn TableBuilder>>;

    async fn remove_table(&self, table_number: u64) -> Result<()>;
}
