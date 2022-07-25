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

use engula_api::server::v1::{group_request_union::Request, group_response_union::Response, *};
use engula_client::GroupClient;

use crate::{Provider, Result};

#[derive(Debug)]
pub struct ForwardCtx {
    pub shard_id: u64,
    pub dest_group_id: u64,
    pub payloads: Vec<ShardData>,
}

pub(crate) async fn forward_request(
    provider: Arc<Provider>,
    forward_ctx: &ForwardCtx,
    request: &Request,
) -> Result<Response> {
    // FIXME(walter) performance
    let group_id = forward_ctx.dest_group_id;
    let mut group_client = GroupClient::new(
        group_id,
        provider.router.clone(),
        provider.conn_manager.clone(),
    );
    let req = ForwardRequest {
        shard_id: forward_ctx.shard_id,
        group_id,
        forward_data: forward_ctx.payloads.clone(),
        request: Some(GroupRequestUnion {
            request: Some(request.clone()),
        }),
    };
    let resp = group_client.forward(req).await?;
    let resp = resp.response.and_then(|resp| resp.response);
    Ok(resp.unwrap())
}
