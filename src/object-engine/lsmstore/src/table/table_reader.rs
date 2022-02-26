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

use super::{table_footer, BlockHandle, TableFooter};
use crate::Result;

#[allow(dead_code)]
pub struct TableReader<R> {
    reader: Reader<R>,
    index_block: Vec<u8>,
}

#[allow(dead_code)]
impl<R> TableReader<R>
where
    R: RandomRead,
{
    pub async fn open(reader: R, table_size: usize) -> Result<Self> {
        let reader = Reader::new(reader);
        let footer = reader.read_footer(table_size).await?;
        let index_block = reader.read_block(&footer.index_handle).await?;
        Ok(Self {
            reader,
            index_block,
        })
    }
}

#[derive(Clone)]
struct Reader<R> {
    reader: Arc<R>,
}

impl<R> Reader<R>
where
    R: RandomRead,
{
    fn new(reader: R) -> Self {
        Self {
            reader: Arc::new(reader),
        }
    }

    async fn read_block(&self, handle: &BlockHandle) -> Result<Vec<u8>> {
        let mut buf = Vec::with_capacity(handle.length as usize);
        self.reader.read_at(&mut buf, handle.offset).await?;
        Ok(buf)
    }

    async fn read_footer(&self, table_size: usize) -> Result<TableFooter> {
        let mut buf = [0; table_footer::ENCODED_SIZE];
        let offset = table_size - buf.len();
        self.reader.read_at(&mut buf, offset as u64).await?;
        TableFooter::decode_from(&mut buf.as_slice())
    }
}
