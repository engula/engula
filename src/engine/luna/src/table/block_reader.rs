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

use engula_futures::io::{RandomRead, RandomReadExt};

use super::block_iter::BlockIter;
use crate::{table::block_handle::BlockHandle, Result};

pub struct BlockReader {
    data: Arc<[u8]>,
}

impl BlockReader {
    pub fn new(content: Vec<u8>) -> Self {
        BlockReader {
            data: content.into(),
        }
    }

    pub fn iter(&self) -> BlockIter {
        BlockIter::new(self.data.clone())
    }
}

pub async fn read_block<R>(reader: &R, block_handle: &BlockHandle) -> Result<Vec<u8>>
where
    R: RandomRead + Unpin,
{
    let mut content = vec![0u8; block_handle.length as usize];
    reader
        .read_exact(&mut content, block_handle.offset as usize)
        .await?;
    Ok(content)
}
