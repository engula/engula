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
use object_engine_master::proto::*;

use crate::{Bucket, Env, Error, Result, SstBuilder};

pub struct BulkLoad<E: Env> {
    env: E,
    token: String,
    output_files: Vec<BulkLoadFileDesc>,
}

impl<E: Env> BulkLoad<E> {
    pub(crate) fn new(env: E, token: String) -> Self {
        Self {
            env,
            token,
            output_files: Vec::new(),
        }
    }

    pub async fn new_sst_builder(&self, bucket: &Bucket<E>) -> Result<SstBuilder> {
        let file_name = self.allocate_file_name().await?;
        let file_writer = bucket.new_sequential_writer(&file_name).await?;
        let table_options = TableBuilderOptions::default();
        let table_builder = TableBuilder::new(file_writer, table_options);
        Ok(SstBuilder::new(
            self.token.clone(),
            bucket.name().to_owned(),
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
        let req = request_union::Request::CommitBulkload(req);
        let res = self.env.handle_union(req).await?;
        if let response_union::Response::CommitBulkload(_) = res {
            Ok(())
        } else {
            Err(Error::internal("missing commit bulkload response"))
        }
    }

    async fn allocate_file_name(&self) -> Result<String> {
        let req = AllocateFileNamesRequest {
            token: self.token.clone(),
            count: 1,
        };
        let req = request_union::Request::AllocateFileNames(req);
        let res = self.env.handle_union(req).await?;
        if let response_union::Response::AllocateFileNames(mut res) = res {
            res.names
                .pop()
                .ok_or_else(|| Error::internal("missing file names"))
        } else {
            Err(Error::internal("missing allocate file names response"))
        }
    }
}
