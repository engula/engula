use async_trait::async_trait;

use super::Storage;
use crate::{
    error::Result,
    format::{ParquetBuilder, ParquetOptions, ParquetReader, TableBuilder, TableDesc, TableReader},
    fs::{open_fs, Fs},
};

fn parquet_name(number: u64) -> String {
    format!("{}.parquet", number)
}

pub struct ParquetStorage {
    fs: Box<dyn Fs>,
    options: ParquetOptions,
}

impl ParquetStorage {
    #[allow(dead_code)]
    pub async fn new(url: &str, options: ParquetOptions) -> Result<ParquetStorage> {
        let fs = open_fs(url).await?;
        Ok(ParquetStorage { fs, options })
    }
}

#[async_trait]
impl Storage for ParquetStorage {
    async fn new_reader(&self, desc: TableDesc) -> Result<Box<dyn TableReader>> {
        let file_name = parquet_name(desc.table_number);
        let file = self.fs.new_random_access_reader(&file_name).await?;
        let reader = ParquetReader::new(file, desc)?;
        Ok(Box::new(reader))
    }

    async fn new_builder(&self, table_number: u64) -> Result<Box<dyn TableBuilder>> {
        let file_name = parquet_name(table_number);
        let file = self.fs.new_sequential_writer(&file_name).await?;
        let builder = ParquetBuilder::new(self.options.clone(), file, table_number);
        Ok(Box::new(builder))
    }

    async fn remove_table(&self, table_number: u64) -> Result<()> {
        let file_name = parquet_name(table_number);
        self.fs.remove_file(&file_name).await
    }
}
