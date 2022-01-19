// Copyright 2021 The Engula Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use engula_futures::io::{SequentialWrite, SequentialWriteExt};

use super::{block_builder::BlockBuilder, block_handle::BlockHandle};
use crate::Result;

// Table format:
//
// Table = { DataBlock } IndexBlock TableFooter
// DataBlock = Block
// IndexBlock = Block
//
// TableFooter format:
//   index block handle : BlockHandle

pub struct TableBuilderOptions {
    pub block_size: u32,
    pub block_restart_interval: u32,
}

impl Default for TableBuilderOptions {
    fn default() -> Self {
        Self {
            block_size: 4096,
            block_restart_interval: 16,
        }
    }
}

#[allow(dead_code)]
pub struct TableBuilder<W> {
    options: TableBuilderOptions,
    last_key: Vec<u8>,
    table_writer: TableWriter<W>,
    data_block_builder: BlockBuilder,
    index_block_builder: BlockBuilder,
}

#[allow(dead_code)]
impl<W: SequentialWrite + Unpin> TableBuilder<W> {
    pub fn new(options: TableBuilderOptions, writer: W) -> Self {
        let data_block_builder = BlockBuilder::new(options.block_restart_interval);
        let index_block_builder = BlockBuilder::new(1);
        Self {
            options,
            last_key: Vec::new(),
            table_writer: TableWriter::new(writer),
            data_block_builder,
            index_block_builder,
        }
    }

    pub async fn add(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.last_key = key.to_owned();
        self.data_block_builder.add(key, value);
        if self.data_block_builder.estimated_size() >= self.options.block_size {
            self.finish_data_block().await?;
        }
        Ok(())
    }

    pub async fn finish(mut self) -> Result<u64> {
        self.finish_data_block().await?;
        let block = self.index_block_builder.finish();
        let handle = self.table_writer.write(block).await?;
        self.table_writer.write(&handle.encode_to_vec()).await?;
        self.table_writer.close().await?;
        Ok(self.table_writer.offset())
    }

    async fn finish_data_block(&mut self) -> Result<()> {
        if self.data_block_builder.num_entries() > 0 {
            let block = self.data_block_builder.finish();
            let handle = self.table_writer.write(block).await?;
            self.data_block_builder.reset();
            self.index_block_builder
                .add(&self.last_key, &handle.encode_to_vec());
        }
        Ok(())
    }
}

struct TableWriter<W> {
    writer: W,
    offset: u64,
}

impl<W: SequentialWrite + Unpin> TableWriter<W> {
    fn new(writer: W) -> Self {
        Self { writer, offset: 0 }
    }

    fn offset(&self) -> u64 {
        self.offset
    }

    async fn write(&mut self, data: &[u8]) -> Result<BlockHandle> {
        let handle = BlockHandle::new(self.offset, data.len() as u64);
        self.writer.write_all(data).await?;
        self.offset += data.len() as u64;
        Ok(handle)
    }

    async fn close(&mut self) -> Result<()> {
        self.writer.close().await?;
        Ok(())
    }
}
