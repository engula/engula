use std::sync::Arc;

use async_trait::async_trait;

use super::Storage;
use crate::{
    error::Result,
    format::{
        SstableBuilder, SstableOptions, SstableReader, TableBuilder, TableDesc, TableReader,
        TableReaderOptions,
    },
    fs::Fs,
};

fn sstable_name(number: u64) -> String {
    format!("{}.sstable", number)
}

pub struct SstableStorage {
    fs: Arc<dyn Fs>,
    options: SstableOptions,
}

impl SstableStorage {
    pub fn new(fs: Arc<dyn Fs>, options: SstableOptions) -> SstableStorage {
        SstableStorage { fs, options }
    }
}

#[async_trait]
impl Storage for SstableStorage {
    async fn new_reader(
        &self,
        desc: TableDesc,
        options: TableReaderOptions,
    ) -> Result<Box<dyn TableReader>> {
        let file_name = sstable_name(desc.table_number);
        let file = self.fs.new_random_access_reader(&file_name).await?;
        let reader = SstableReader::new(file, desc, options).await?;
        Ok(Box::new(reader))
    }

    async fn new_builder(&self, table_number: u64) -> Result<Box<dyn TableBuilder>> {
        let file_name = sstable_name(table_number);
        let file = self.fs.new_sequential_writer(&file_name).await?;
        let builder = SstableBuilder::new(self.options.clone(), file, table_number);
        Ok(Box::new(builder))
    }

    async fn count_table(&self, table_number: u64) -> Result<usize> {
        let file_name = sstable_name(table_number);
        self.fs.count_file(&file_name).await
    }

    async fn remove_table(&self, table_number: u64) -> Result<()> {
        let file_name = sstable_name(table_number);
        self.fs.remove_file(&file_name).await
    }
}
