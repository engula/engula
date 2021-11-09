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

use std::{cmp::min, sync::Arc};

use bytes::BufMut;

use crate::{async_trait, Error, Result, StorageObject};

#[derive(Clone)]
pub struct MemObject {
    data: Arc<Vec<u8>>,
}

impl MemObject {
    pub fn new(data: Vec<u8>) -> MemObject {
        MemObject {
            data: Arc::new(data),
        }
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }
}

#[async_trait]
impl StorageObject for MemObject {
    async fn size(&self) -> Result<usize> {
        Ok(self.data.len())
    }

    async fn read_at(&self, mut buf: &mut [u8], offset: usize) -> Result<usize> {
        if let Some(length) = self.data.len().checked_sub(offset) {
            let length = min(length, buf.len());
            buf.put_slice(&self.data[offset..(offset + length)]);
            Ok(length)
        } else {
            Err(Error::InvalidArgument(format!(
                "offset {} > object length {}",
                offset,
                self.data.len()
            )))
        }
    }
}
