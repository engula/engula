use std::sync::Arc;

use async_trait::async_trait;

use super::{CompactionInput, CompactionOutput, CompactionRuntime};
use crate::{
    error::Result,
    format::{Iterator, MergingIterator},
    storage::Storage,
};

pub struct LocalCompaction {
    storage: Arc<dyn Storage>,
}

impl LocalCompaction {
    pub fn new(storage: Arc<dyn Storage>) -> LocalCompaction {
        LocalCompaction { storage }
    }
}

#[async_trait]
impl CompactionRuntime for LocalCompaction {
    async fn compact(&self, input: CompactionInput) -> Result<CompactionOutput> {
        let mut children = Vec::new();
        for desc in &input.tables {
            let reader = self.storage.new_reader(desc).await?;
            let iter = reader.new_iterator();
            children.push(iter);
        }
        let mut iter = MergingIterator::new(children);
        let mut builder = self.storage.new_builder(input.output_table_number).await?;
        iter.seek_to_first().await;
        while let Some(v) = iter.current()? {
            builder.add(v.0, v.1, v.2).await;
            iter.next().await;
        }
        let output_table = builder.finish().await?;
        Ok(CompactionOutput {
            tables: input.tables,
            output_table: Some(output_table),
        })
    }
}
