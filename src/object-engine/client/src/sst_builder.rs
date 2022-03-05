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

use object_engine_lsmstore::{Key, TableBuilder, Timestamp, ValueType};
use object_engine_master::proto::BulkLoadFileDesc;

use crate::Result;

pub struct SstBuilder {
    token: String,
    bucket: String,
    file_name: String,
    table_builder: TableBuilder,
}

impl SstBuilder {
    pub(crate) fn new(
        token: String,
        bucket: String,
        file_name: String,
        table_builder: TableBuilder,
    ) -> Self {
        Self {
            token,
            bucket,
            file_name,
            table_builder,
        }
    }

    pub(crate) fn token(&self) -> &str {
        &self.token
    }

    async fn add(&mut self, id: &[u8], ts: Timestamp, tp: ValueType, value: &[u8]) -> Result<()> {
        let key = Key::encode_to_vec(id, ts, tp);
        self.table_builder.add(key.as_slice().into(), value).await
    }

    pub async fn put(&mut self, id: &[u8], ts: Timestamp, value: &[u8]) -> Result<()> {
        self.add(id, ts, ValueType::Put, value).await
    }

    pub async fn delete(&mut self, id: &[u8], ts: Timestamp) -> Result<()> {
        self.add(id, ts, ValueType::Delete, &[]).await
    }

    pub fn estimated_size(&self) -> usize {
        self.table_builder.estimated_size()
    }

    pub(crate) async fn finish(self) -> Result<BulkLoadFileDesc> {
        let desc = self.table_builder.finish().await?;
        Ok(BulkLoadFileDesc {
            bucket: self.bucket,
            file_name: self.file_name,
            file_size: desc.table_size as u64,
            lower_bound: desc.lower_bound,
            upper_bound: desc.upper_bound,
        })
    }
}
