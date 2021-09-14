mod hybrid_storage;
mod parquet_storage;
mod sst_storage;

pub use hybrid_storage::HybridStorage;
pub use parquet_storage::ParquetStorage;
pub use sst_storage::SstStorage;

use async_trait::async_trait;

use crate::{
    error::Result,
    format::{TableBuilder, TableDesc, TableReader},
};

#[async_trait]
pub trait Storage: Send + Sync {
    async fn new_reader(&self, desc: TableDesc) -> Result<Box<dyn TableReader>>;

    async fn new_builder(&self, table_number: u64) -> Result<Box<dyn TableBuilder>>;

    async fn remove_table(&self, table_number: u64) -> Result<()>;
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::format::{Entry, ParquetOptions, SstOptions};

    #[tokio::test]
    async fn test() {
        const NUM: u64 = 1000;
        let sst_url = "file:///tmp/engula_test/sst";
        let sst_options = SstOptions::default();
        let sst_storage = SstStorage::new(sst_url, sst_options).await.unwrap();
        let sst_storage: Arc<dyn Storage> = Arc::new(sst_storage);

        let parquet_url = "file:///tmp/engula_test/parquet";
        let parquet_options = ParquetOptions::default();
        let parquet_storage = ParquetStorage::new(parquet_url, parquet_options)
            .await
            .unwrap();
        let parquet_storage: Arc<dyn Storage> = Arc::new(parquet_storage);

        let hybrid_storage = HybridStorage::new(
            sst_storage.clone(),
            vec![sst_storage.clone(), parquet_storage.clone()],
        );
        let hybrid_storage: Arc<dyn Storage> = Arc::new(hybrid_storage);

        let storages = vec![sst_storage, parquet_storage, hybrid_storage];
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
