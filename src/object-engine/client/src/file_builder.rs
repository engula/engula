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

use object_engine_filestore::SequentialWrite;
use object_engine_lsmstore::{TableBuilder, TableBuilderOptions};
use object_engine_master::proto::BulkLoadFileDesc;

use crate::Result;

pub struct FileBuilder {
    bucket: String,
    file_name: String,
    table_builder: TableBuilder,
}

impl FileBuilder {
    pub(crate) fn new(
        bucket: String,
        file_name: String,
        file_writer: Box<dyn SequentialWrite>,
    ) -> Self {
        let table_options = TableBuilderOptions::default();
        let table_builder = TableBuilder::new(file_writer, table_options);
        Self {
            bucket,
            file_name,
            table_builder,
        }
    }

    pub async fn finish(self) -> Result<BulkLoadFileDesc> {
        let desc = self.table_builder.finish().await?;
        Ok(BulkLoadFileDesc {
            bucket: self.bucket,
            file_name: self.file_name,
            file_size: desc.table_size,
            lower_bound: desc.lower_bound,
            upper_bound: desc.upper_bound,
        })
    }
}
