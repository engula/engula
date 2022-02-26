// Copyright 2022 The Engula Authors.
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

use object_engine_filestore::SequentialWrite;

use super::{BlockBuilder, BlockHandle};
use crate::Result;

#[derive(Default)]
pub struct TableDesc {
    pub table_size: u64,
    pub lower_bound: Vec<u8>,
    pub upper_bound: Vec<u8>,
}

pub struct TableBuilderOptions {
    pub block_size: u64,
}

impl Default for TableBuilderOptions {
    fn default() -> Self {
        Self { block_size: 8192 }
    }
}

pub struct TableBuilder<W> {
    writer: TableWriter<W>,
    options: TableBuilderOptions,
    lower_bound: Vec<u8>,
    upper_bound: Vec<u8>,
    data_block_builder: BlockBuilder,
    index_block_builder: BlockBuilder,
}

#[allow(dead_code)]
impl<W> TableBuilder<W>
where
    W: SequentialWrite,
{
    pub fn new(writer: W) -> Self {
        Self {
            writer: TableWriter::new(writer),
            options: TableBuilderOptions::default(),
            lower_bound: Vec::new(),
            upper_bound: Vec::new(),
            data_block_builder: BlockBuilder::default(),
            index_block_builder: BlockBuilder::default(),
        }
    }

    pub async fn add(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        if self.lower_bound.is_empty() {
            self.lower_bound = key.to_owned();
        }
        self.upper_bound = key.to_owned();
        self.data_block_builder.add(key, value);
        if self.data_block_builder.encoded_size() >= self.options.block_size as usize {
            self.finish_data_block().await?;
        }
        Ok(())
    }

    pub async fn finish(mut self) -> Result<TableDesc> {
        self.finish_data_block().await?;
        self.finish_index_block().await?;
        self.writer.finish().await?;
        Ok(TableDesc {
            table_size: self.writer.offset(),
            lower_bound: self.lower_bound,
            upper_bound: self.upper_bound,
        })
    }

    async fn finish_data_block(&mut self) -> Result<()> {
        if self.data_block_builder.num_entries() > 0 {
            let block = self.data_block_builder.finish();
            let handle = self.writer.write_block(block).await?;
            let index_value = handle.encode_to_vec();
            self.index_block_builder
                .add(&self.upper_bound, &index_value);
            self.data_block_builder.reset();
        }
        Ok(())
    }

    async fn finish_index_block(&mut self) -> Result<()> {
        if self.index_block_builder.num_entries() > 0 {
            let block = self.index_block_builder.finish();
            let handle = self.writer.write_block(block).await?;
            let footer = handle.encode_to_vec();
            self.writer.write(&footer).await?;
            self.index_block_builder.reset();
        }
        Ok(())
    }
}

struct TableWriter<W> {
    writer: W,
    offset: u64,
}

impl<W> TableWriter<W>
where
    W: SequentialWrite,
{
    fn new(writer: W) -> Self {
        Self { writer, offset: 0 }
    }

    fn offset(&self) -> u64 {
        self.offset
    }

    async fn write(&mut self, buf: &[u8]) -> Result<()> {
        self.writer.write(buf).await?;
        self.offset += buf.len() as u64;
        Ok(())
    }

    async fn write_block(&mut self, block: &[u8]) -> Result<BlockHandle> {
        let handle = BlockHandle {
            offset: self.offset,
            length: block.len() as u64,
        };
        self.write(block).await?;
        Ok(handle)
    }

    async fn finish(&mut self) -> Result<()> {
        self.writer.flush().await
    }
}
