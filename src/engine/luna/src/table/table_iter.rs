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

use engula_futures::io::RandomRead;

use crate::{
    table::{
        block_handle::BlockHandle,
        block_iter::BlockIter,
        block_reader::{read_block, BlockReader},
    },
    Result,
};

async fn read_block_iter<R: RandomRead + Unpin>(
    reader: &mut R,
    mut value: &[u8],
) -> Result<BlockIter> {
    let handle = BlockHandle::decode_from(&mut value);
    let block_content = read_block(reader, &handle).await?;
    let block = BlockReader::new(block_content);
    Ok(block.iter())
}

pub(crate) struct TableIter<'a, R> {
    reader: &'a mut R,
    index_iter: BlockIter,
    block_iter: Option<BlockIter>,
}

impl<'a, R: RandomRead + Unpin> TableIter<'a, R> {
    pub fn new(index_iter: BlockIter, reader: &'a mut R) -> Self {
        TableIter {
            reader,
            index_iter,
            block_iter: None,
        }
    }

    #[allow(dead_code)]
    pub async fn seek_to_first(&mut self) -> Result<()> {
        self.index_iter.seek_to_first();
        self.block_iter = if self.index_iter.valid() {
            let mut iter = read_block_iter(&mut self.reader, self.index_iter.value()).await?;
            iter.seek_to_first();
            Some(iter)
        } else {
            None
        };
        Ok(())
    }

    pub async fn seek(&mut self, target: &[u8]) -> Result<()> {
        self.index_iter.seek(target);
        self.block_iter = if self.index_iter.valid() {
            let mut iter = read_block_iter(&mut self.reader, self.index_iter.value()).await?;
            iter.seek(target);
            Some(iter)
        } else {
            None
        };
        Ok(())
    }

    #[allow(dead_code)]
    pub async fn next(&mut self) -> Result<()> {
        let valid = match &mut self.block_iter {
            Some(iter) => {
                iter.next();
                iter.valid()
            }
            None => false,
        };
        if !valid {
            self.index_iter.next();
            if self.index_iter.valid() {
                let mut iter = read_block_iter(&mut self.reader, self.index_iter.value()).await?;
                iter.seek_to_first();
                self.block_iter = Some(iter);
            } else {
                self.block_iter = None;
            }
        }

        Ok(())
    }

    pub fn valid(&self) -> bool {
        self.block_iter
            .as_ref()
            .map(|iter| iter.valid())
            .unwrap_or_default()
    }

    pub fn key(&self) -> &[u8] {
        self.block_iter.as_ref().unwrap().key()
    }

    pub fn value(&self) -> &[u8] {
        self.block_iter.as_ref().unwrap().value()
    }
}
