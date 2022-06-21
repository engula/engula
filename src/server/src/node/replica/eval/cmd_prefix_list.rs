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

use engula_api::server::v1::{ShardPrefixListRequest, ShardPrefixListResponse};

use crate::{node::engine::GroupEngine, Result};

/// List the key-value pairs of the specified key prefix.
pub async fn prefix_list(
    engine: &GroupEngine,
    req: &ShardPrefixListRequest,
) -> Result<ShardPrefixListResponse> {
    let prefix = &req.prefix;
    let mut values = Vec::new();
    let iter = engine.iter_from(prefix.to_owned())?;
    for (key, value) in iter {
        if !key.starts_with(prefix) {
            break;
        }
        values.push(value.to_vec());
    }
    Ok(ShardPrefixListResponse { values })
}
