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

use super::{
    block_handle::BlockHandle,
    block_reader::{read_block, BlockReader},
    block_scanner::BlockScanner,
    RandomReader,
};
use crate::Result;

async fn read_block_scanner(reader: &RandomReader, mut value: &[u8]) -> Result<BlockScanner> {
    let handle = BlockHandle::decode_from(&mut value);
    let block_content = read_block(reader, &handle).await?;
    let block = BlockReader::new(block_content);
    Ok(block.scan())
}

pub struct TableScanner {
    reader: RandomReader,
    index_scanner: BlockScanner,
    block_scanner: Option<BlockScanner>,
}

impl TableScanner {
    pub fn new(reader: RandomReader, index_scanner: BlockScanner) -> Self {
        TableScanner {
            reader,
            index_scanner,
            block_scanner: None,
        }
    }

    #[allow(dead_code)]
    pub async fn seek_to_first(&mut self) -> Result<()> {
        self.index_scanner.seek_to_first();
        self.block_scanner = if self.index_scanner.valid() {
            let mut scanner = read_block_scanner(&self.reader, self.index_scanner.value()).await?;
            scanner.seek_to_first();
            Some(scanner)
        } else {
            None
        };
        Ok(())
    }

    pub async fn seek(&mut self, target: &[u8]) -> Result<()> {
        self.index_scanner.seek(target);
        self.block_scanner = if self.index_scanner.valid() {
            let mut scanner = read_block_scanner(&self.reader, self.index_scanner.value()).await?;
            scanner.seek(target);
            Some(scanner)
        } else {
            None
        };
        Ok(())
    }

    #[allow(dead_code)]
    pub async fn next(&mut self) -> Result<()> {
        let valid = match &mut self.block_scanner {
            Some(scanner) => {
                scanner.next();
                scanner.valid()
            }
            None => false,
        };
        if !valid {
            self.index_scanner.next();
            if self.index_scanner.valid() {
                let mut scanner =
                    read_block_scanner(&self.reader, self.index_scanner.value()).await?;
                scanner.seek_to_first();
                self.block_scanner = Some(scanner);
            } else {
                self.block_scanner = None;
            }
        }

        Ok(())
    }

    pub fn valid(&self) -> bool {
        self.block_scanner
            .as_ref()
            .map(|scanner| scanner.valid())
            .unwrap_or_default()
    }

    pub fn key(&self) -> &[u8] {
        self.block_scanner.as_ref().unwrap().key()
    }

    pub fn value(&self) -> &[u8] {
        self.block_scanner.as_ref().unwrap().value()
    }
}
