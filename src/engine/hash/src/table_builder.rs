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

use bytes::BufMut;
use tokio::io::{AsyncWrite, AsyncWriteExt};

use crate::Result;

pub struct TableBuilder<W: AsyncWrite + Unpin> {
    write: W,
    block: BlockBuilder,
}

#[allow(dead_code)]
impl<W: AsyncWrite + Unpin> TableBuilder<W> {
    pub fn new(write: W) -> Self {
        Self {
            write,
            block: BlockBuilder::new(),
        }
    }

    pub async fn add(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.block.add(key, value);
        if self.block.size() >= BLOCK_SIZE {
            self.flush().await?;
        }
        Ok(())
    }

    async fn flush(&mut self) -> Result<()> {
        self.write.write_all(self.block.data()).await?;
        self.block.reset();
        Ok(())
    }

    pub async fn finish(mut self) -> Result<()> {
        if self.block.size() > 0 {
            self.flush().await?;
        }
        self.write.shutdown().await?;
        Ok(())
    }
}

const BLOCK_SIZE: usize = 8 * 1024;

struct BlockBuilder {
    buf: Vec<u8>,
}

impl BlockBuilder {
    fn new() -> Self {
        Self {
            buf: Vec::with_capacity(BLOCK_SIZE),
        }
    }

    fn add(&mut self, key: &[u8], value: &[u8]) {
        self.buf.put_u64(key.len() as u64);
        self.buf.put_slice(key);
        self.buf.put_u64(value.len() as u64);
        self.buf.put_slice(value);
    }

    fn data(&self) -> &[u8] {
        &self.buf
    }

    fn size(&self) -> usize {
        self.buf.len()
    }

    fn reset(&mut self) {
        self.buf.clear()
    }
}
