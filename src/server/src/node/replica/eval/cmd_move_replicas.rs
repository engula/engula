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
use tracing::info;

use crate::{node::replica::ExecCtx, schedule::MoveReplicasProvider, Result};

pub async fn move_replicas(
    ctx: &ExecCtx,
    provider: &MoveReplicasProvider,
    req: &MoveReplicasRequest,
) -> Result<()> {
    let incoming_voters = req.incoming_voters.clone();
    let outgoing_voters = req.outgoing_voters.clone();

    info!(
        "group {} replica {} receive moving replicas requests, incoming {:?}, outgoing {:?}",
        ctx.group_id,
        ctx.replica_id,
        incoming_voters.iter().map(|v| v.id).collect::<Vec<_>>(),
        outgoing_voters.iter().map(|v| v.id).collect::<Vec<_>>()
    );

    provider
        .assign(ctx.epoch, incoming_voters, outgoing_voters)
        .await??;

    Ok(())
}
