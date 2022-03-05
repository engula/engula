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

use object_engine_lsmstore::{TableBuilder, TableBuilderOptions};
use object_engine_master::{proto::*, FileTenant};

use crate::{Error, Master, Result, SstBuilder};

pub struct BulkLoad {
    token: String,
    tenant: String,
    master: Master,
    file_tenant: FileTenant,
    output_files: Vec<BulkLoadFileDesc>,
}

impl BulkLoad {
    pub(crate) fn new(
        token: String,
        tenant: String,
        master: Master,
        file_tenant: FileTenant,
    ) -> Self {
        Self {
            token,
            tenant,
            master,
            file_tenant,
            output_files: Vec::new(),
        }
    }

    pub async fn new_sst_builder(&self, bucket: &str) -> Result<SstBuilder> {
        let file_name = self.allocate_file_name().await?;
        let file_writer = self
            .file_tenant
            .new_sequential_writer(bucket, &file_name)
            .await?;
        let table_options = TableBuilderOptions::default();
        let table_builder = TableBuilder::new(file_writer, table_options);
        Ok(SstBuilder::new(
            self.token.clone(),
            bucket.to_owned(),
            file_name,
            table_builder,
        ))
    }

    pub async fn finish_sst_builder(&mut self, builder: SstBuilder) -> Result<()> {
        if builder.token() != self.token {
            return Err(Error::invalid_argument(
                "the file doesn't belong to this bulkload",
            ));
        }
        let desc = builder.finish().await?;
        self.output_files.push(desc);
        Ok(())
    }

    pub async fn commit(self) -> Result<()> {
        let req = CommitBulkLoadRequest {
            token: self.token,
            files: self.output_files,
        };
        self.master.commit_bulkload(self.tenant, req).await
    }

    async fn allocate_file_name(&self) -> Result<String> {
        let mut file_names = self
            .master
            .allocate_file_names(self.tenant.clone(), self.token.clone(), 1)
            .await?;
        file_names
            .pop()
            .ok_or_else(|| Error::internal("missing file names"))
    }
}
