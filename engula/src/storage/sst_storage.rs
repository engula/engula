use std::sync::Arc;

use async_trait::async_trait;

use super::Storage;
use crate::{
    cache::Cache,
    error::Result,
    format::{SstBuilder, SstOptions, SstReader, TableBuilder, TableDesc, TableReader},
    fs::Fs,
};

fn sst_name(number: u64) -> String {
    format!("{}.sst", number)
}

pub struct SstStorage {
    options: SstOptions,
    fs: Arc<dyn Fs>,
    cache: Option<Arc<dyn Cache>>,
}

impl SstStorage {
    pub fn new(options: SstOptions, fs: Arc<dyn Fs>, cache: Option<Arc<dyn Cache>>) -> SstStorage {
        SstStorage { options, fs, cache }
    }
}

#[async_trait]
impl Storage for SstStorage {
    async fn new_reader(&self, desc: &TableDesc) -> Result<Box<dyn TableReader>> {
        let file_name = sst_name(desc.table_number);
        let file = self.fs.new_random_access_reader(&file_name).await?;
        let reader = SstReader::open(desc, file, self.cache.clone()).await?;
        Ok(Box::new(reader))
    }

    async fn new_builder(&self, table_number: u64) -> Result<Box<dyn TableBuilder>> {
        let file_name = sst_name(table_number);
        let file = self.fs.new_sequential_writer(&file_name).await?;
        let builder = SstBuilder::new(self.options.clone(), file, table_number);
        Ok(Box::new(builder))
    }

    async fn remove_table(&self, table_number: u64) -> Result<()> {
        let file_name = sst_name(table_number);
        self.fs.remove_file(&file_name).await
    }
}
