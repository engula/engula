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
use std::path::Path;

use engula_api::server::v1::GroupDesc;

use crate::{
    node::{engine::GroupEngineIterator, replica::raft::SnapshotBuilder, GroupEngine},
    serverpb::v1::ApplyState,
    Result,
};

pub struct GroupSnapshotBuilder {
    engine: GroupEngine,
}

impl GroupSnapshotBuilder {
    pub fn new(engine: GroupEngine) -> Self {
        GroupSnapshotBuilder { engine }
    }
}

#[crate::async_trait]
impl SnapshotBuilder for GroupSnapshotBuilder {
    async fn checkpoint(&self, base_dir: &Path) -> Result<(ApplyState, GroupDesc)> {
        let mut iter = self.engine.iter()?;
        for i in 0.. {
            if write_partial_to_file(&mut iter, base_dir, i)
                .await?
                .is_none()
            {
                break;
            }
        }
        iter.status()?;

        let apply_state = iter.apply_state().clone();
        let descriptor = iter.descriptor().clone();
        Ok((apply_state, descriptor))
    }
}

/// Write partial of the iterator's data to the file, return `None` if all data is written.
async fn write_partial_to_file(
    iter: &mut GroupEngineIterator<'_>,
    base_dir: &Path,
    file_no: usize,
) -> Result<Option<()>> {
    use rocksdb::{Options, SstFileWriter};

    let opts = Options::default();
    let mut writer = SstFileWriter::create(&opts);
    writer.open(base_dir.join(format!("{}.sst", file_no)))?;

    let mut index = 0;
    while let Some((key, value)) = iter.next() {
        writer.put(key, value)?;
        if writer.file_size() > 64 * 1024 * 1024 {
            writer.finish()?;
            return Ok(Some(()));
        }

        index += 1;
        if index % 1024 == 0 {
            tokio::task::yield_now().await;
        }
    }

    Ok(None)
}
