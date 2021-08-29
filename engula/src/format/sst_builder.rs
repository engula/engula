use std::pin::Pin;

use async_trait::async_trait;
use tokio::io::AsyncWriteExt;

use super::block::{BlockBuilder, BlockHandle};
use super::table_builder::TableBuilder;
use crate::common::Timestamp;
use crate::file_system::{SequentialFileReader, SequentialFileWriter};
use crate::Result;

#[derive(Clone)]
pub struct SstOptions {
    pub block_size: usize,
}

impl SstOptions {
    fn default() -> SstOptions {
        SstOptions { block_size: 8192 }
    }
}

pub struct SstBuilder {
    options: SstOptions,
    file: SstFileWriter,
    last_ts: Timestamp,
    last_key: Vec<u8>,
    data_block: BlockBuilder,
    index_block: BlockBuilder,
}

impl SstBuilder {
    fn new(options: SstOptions, file: Box<dyn SequentialFileWriter>) -> SstBuilder {
        SstBuilder {
            options,
            file: SstFileWriter::new(file),
            last_ts: 0,
            last_key: Vec::new(),
            data_block: BlockBuilder::new(),
            index_block: BlockBuilder::new(),
        }
    }

    async fn flush_data_block(&mut self) -> Result<()> {
        let block = self.data_block.finish();
        let block_handle = self.file.write_block(block).await?;
        let encoded_handle = block_handle.encode();
        self.index_block
            .add(self.last_ts, &self.last_key, &encoded_handle);
        Ok(())
    }
}

#[async_trait]
impl TableBuilder for SstBuilder {
    async fn add(&mut self, ts: Timestamp, key: &[u8], value: &[u8]) {
        self.last_ts = ts;
        self.last_key = key.to_owned();
        self.data_block.add(ts, key, value);
        if self.data_block.approximate_size() >= self.options.block_size {
            self.flush_data_block().await.unwrap();
        }
    }

    async fn finish(&mut self) -> Result<()> {
        if self.data_block.approximate_size() > 0 {
            self.flush_data_block().await?;
        }
        if self.index_block.approximate_size() > 0 {
            let block = self.index_block.finish();
            let _ = self.file.write_block(block).await?;
        }
        Ok(())
    }
}

struct SstFileWriter {
    file: Pin<Box<dyn SequentialFileWriter>>,
    offset: usize,
}

impl SstFileWriter {
    fn new(file: Box<dyn SequentialFileWriter>) -> SstFileWriter {
        SstFileWriter {
            file: Pin::new(file),
            offset: 0,
        }
    }

    async fn write_block(&mut self, block: &[u8]) -> Result<BlockHandle> {
        let handle = BlockHandle {
            offset: self.offset as u64,
            size: block.len() as u64,
        };
        self.file.write_all(block).await?;
        self.offset += block.len();
        Ok(handle)
    }
}
