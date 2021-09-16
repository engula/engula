mod hybrid_storage;
mod parquet_storage;
mod sstable_storage;

pub use hybrid_storage::HybridStorage;
pub use parquet_storage::ParquetStorage;
pub use sstable_storage::SstableStorage;

use async_trait::async_trait;

use crate::{
    error::Result,
    format::{TableBuilder, TableDesc, TableReader},
};

#[async_trait]
pub trait Storage: Send + Sync {
    async fn new_reader(&self, desc: TableDesc) -> Result<Box<dyn TableReader>>;

    async fn new_builder(&self, table_number: u64) -> Result<Box<dyn TableBuilder>>;

    async fn count_table(&self, table_number: u64) -> Result<usize>;

    async fn remove_table(&self, table_number: u64) -> Result<()>;
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::{
        format::{Entry, ParquetOptions, SstableOptions},
        fs::LocalFs,
    };

    #[tokio::test]
    async fn test() {
        const NUM: u64 = 1000;
        let dirname = "/tmp/engula_test/storage";
        let _ = std::fs::remove_dir_all(dirname);

        let fs = Arc::new(LocalFs::new(dirname).unwrap());
        let sstable_options = SstableOptions::default();
        let sstable_storage = SstableStorage::new(fs.clone(), sstable_options);
        let sstable_storage: Arc<dyn Storage> = Arc::new(sstable_storage);

        let parquet_options = ParquetOptions::default();
        let parquet_storage = ParquetStorage::new(fs.clone(), parquet_options);
        let parquet_storage: Arc<dyn Storage> = Arc::new(parquet_storage);

        let hybrid_storage = HybridStorage::new(
            sstable_storage.clone(),
            vec![sstable_storage.clone(), parquet_storage.clone()],
        );
        let hybrid_storage: Arc<dyn Storage> = Arc::new(hybrid_storage);

        let storages = vec![sstable_storage, parquet_storage, hybrid_storage];
        for storage in storages {
            let mut builder = storage.new_builder(123).await.unwrap();
            for i in 0..NUM {
                let v = i.to_be_bytes();
                builder.add(i, &v, &v).await;
            }
            let desc = builder.finish().await.unwrap();
            let reader = storage.new_reader(desc.clone()).await.unwrap();
            let mut iter = reader.new_iterator();
            iter.seek_to_first().await;
            for i in 0..NUM {
                let v = i.to_be_bytes();
                assert_eq!(iter.current().unwrap(), Some(Entry(i, &v, &v)));
                iter.next().await;
            }
            assert_eq!(iter.current().unwrap(), None);
            storage.remove_table(desc.table_number).await.unwrap();
        }
    }
}
