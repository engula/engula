use std::sync::Arc;

use async_trait::async_trait;
use prost::Message;

use super::Storage;
use crate::{
    error::Result,
    format::{TableBuilder, TableDesc, TableReader, Timestamp},
};

pub struct HybridStorage {
    read: Arc<dyn Storage>,
    writes: Vec<Arc<dyn Storage>>,
}

impl HybridStorage {
    #[allow(dead_code)]
    pub fn new(read: Arc<dyn Storage>, writes: Vec<Arc<dyn Storage>>) -> HybridStorage {
        HybridStorage { read, writes }
    }
}

#[async_trait]
impl Storage for HybridStorage {
    async fn new_reader(&self, desc: TableDesc) -> Result<Box<dyn TableReader>> {
        self.read.new_reader(desc).await
    }

    async fn new_builder(&self, table_number: u64) -> Result<Box<dyn TableBuilder>> {
        let mut builders = Vec::new();
        for s in &self.writes {
            let builder = s.new_builder(table_number).await?;
            builders.push(builder);
        }
        let builder = HybridBuilder::new(builders);
        Ok(Box::new(builder))
    }

    async fn count_table(&self, table_number: u64) -> Result<usize> {
        self.read.count_table(table_number).await
    }

    async fn remove_table(&self, table_number: u64) -> Result<()> {
        for s in &self.writes {
            s.remove_table(table_number).await?;
        }
        Ok(())
    }
}

struct HybridBuilder {
    builders: Vec<Box<dyn TableBuilder>>,
}

impl HybridBuilder {
    fn new(builders: Vec<Box<dyn TableBuilder>>) -> HybridBuilder {
        HybridBuilder { builders }
    }
}

#[async_trait]
impl TableBuilder for HybridBuilder {
    async fn add(&mut self, ts: Timestamp, key: &[u8], value: &[u8]) {
        for builder in &mut self.builders {
            builder.add(ts, key, value).await;
        }
    }

    async fn finish(&mut self) -> Result<TableDesc> {
        let mut final_desc = TableDesc::default();
        for builder in &mut self.builders {
            let desc = builder.finish().await?;
            final_desc.merge(desc.encode_to_vec().as_ref()).unwrap();
        }
        Ok(final_desc)
    }
}
