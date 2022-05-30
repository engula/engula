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
use crate::{
    node::group_engine::{GroupEngine, WriteBatch},
    serverpb::v1::{EvalResult, WriteBatchRep},
    Result,
};

pub async fn delete(group_engine: &GroupEngine, shard_id: u64, key: &[u8]) -> Result<EvalResult> {
    let mut wb = WriteBatch::default();
    group_engine.delete(&mut wb, shard_id, key)?;
    Ok(EvalResult {
        batch: Some(WriteBatchRep {
            data: wb.data().to_owned(),
        }),
    })
}
