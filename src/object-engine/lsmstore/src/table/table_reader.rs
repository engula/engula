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

use std::sync::Arc;

use object_engine_filestore::RandomRead;

use super::{table_footer, BlockHandle, BlockIter, Key, TableFooter};
use crate::Result;

#[allow(dead_code)]
pub struct TableReader {
    reader: FileReader,
    index_block: Arc<[u8]>,
}

#[allow(dead_code)]
impl TableReader {
    pub async fn open(reader: RandomReader, table_size: usize) -> Result<Self> {
        let reader = FileReader::new(reader);
        let footer = reader.read_footer(table_size).await?;
        let index_block = reader.read_block(&footer.index_handle).await?;
        Ok(Self {
            reader,
            index_block,
        })
    }

    pub fn iter(&self) -> TableIter {
        let index_iter = BlockIter::new(self.index_block.clone());
        TableIter::new(self.reader.clone(), index_iter)
    }
}

#[allow(dead_code)]
pub struct TableIter {
    reader: FileReader,
    index_iter: BlockIter,
    block_iter: Option<BlockIter>,
}

#[allow(dead_code)]
impl TableIter {
    fn new(reader: FileReader, index_iter: BlockIter) -> Self {
        Self {
            reader,
            index_iter,
            block_iter: None,
        }
    }

    pub fn key(&self) -> Key<'_> {
        debug_assert!(self.valid());
        self.block_iter.as_ref().unwrap().key()
    }

    pub fn value(&self) -> &[u8] {
        debug_assert!(self.valid());
        self.block_iter.as_ref().unwrap().value()
    }

    pub fn valid(&self) -> bool {
        self.block_iter
            .as_ref()
            .map(|x| x.valid())
            .unwrap_or_default()
    }

    pub async fn seek_to_first(&mut self) -> Result<()> {
        self.index_iter.seek_to_first();
        self.block_iter = if self.index_iter.valid() {
            let mut iter = self.read_block_iter().await?;
            iter.seek_to_first();
            Some(iter)
        } else {
            None
        };
        Ok(())
    }

    pub async fn seek(&mut self, target: Key<'_>) -> Result<()> {
        self.index_iter.seek(target);
        self.block_iter = if self.index_iter.valid() {
            let mut iter = self.read_block_iter().await?;
            iter.seek(target);
            Some(iter)
        } else {
            None
        };
        Ok(())
    }

    pub async fn next(&mut self) -> Result<()> {
        if let Some(mut block_iter) = self.block_iter.take() {
            block_iter.next();
            if block_iter.valid() {
                self.block_iter = Some(block_iter);
            } else {
                self.index_iter.next();
                if self.index_iter.valid() {
                    let mut iter = self.read_block_iter().await?;
                    iter.seek_to_first();
                    self.block_iter = Some(iter);
                } else {
                    self.block_iter = None;
                }
            }
        }
        Ok(())
    }

    async fn read_block_iter(&mut self) -> Result<BlockIter> {
        let mut index_value = self.index_iter.value();
        let handle = BlockHandle::decode_from(&mut index_value);
        let block = self.reader.read_block(&handle).await?;
        Ok(BlockIter::new(block))
    }
}

type RandomReader = Arc<dyn RandomRead>;

#[derive(Clone)]
struct FileReader {
    reader: RandomReader,
}

impl FileReader {
    fn new(reader: RandomReader) -> Self {
        Self { reader }
    }

    async fn read_block(&self, handle: &BlockHandle) -> Result<Arc<[u8]>> {
        let mut buf = Vec::with_capacity(handle.length as usize);
        self.reader.read_exact_at(&mut buf, handle.offset).await?;
        Ok(buf.into())
    }

    async fn read_footer(&self, table_size: usize) -> Result<TableFooter> {
        let mut buf = [0; table_footer::ENCODED_SIZE];
        let offset = table_size - buf.len();
        self.reader.read_exact_at(&mut buf, offset as u64).await?;
        TableFooter::decode_from(&mut buf.as_slice())
    }
}
