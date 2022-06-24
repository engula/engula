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

use engula_api::server::v1::*;
use engula_client::Router;

use super::GroupClient;
use crate::Result;

#[derive(Debug)]
pub struct ForwardCtx {
    pub dest_group_id: u64,
    pub payloads: Vec<ShardData>,
}

pub async fn forward_request(
    router: Router,
    forward_ctx: &ForwardCtx,
    request: &GroupRequest,
) -> Result<GroupResponse> {
    debug_assert!(request.request.is_some());

    let group_id = forward_ctx.dest_group_id;
    let mut group_client = GroupClient::new(group_id, router);
    let req = ForwardRequest {
        group_id,
        forward_data: forward_ctx.payloads.clone(),
        request: request.request.clone(),
    };
    let resp = group_client.forward(req).await?;
    debug_assert!(resp.response.is_some());

    Ok(GroupResponse {
        response: resp.response,
        error: None,
    })
}
