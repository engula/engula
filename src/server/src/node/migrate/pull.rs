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
use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use engula_api::server::v1::*;
use engula_client::Router;
use futures::StreamExt;
use tracing::warn;

use super::GroupClient;
use crate::{node::Replica, serverpb::v1::*, Result};

pub async fn pull_shard(router: Router, replica: Arc<Replica>, migrate_meta: MigrateMeta) {
    let info = replica.replica_info();
    let shard_id = migrate_meta.shard_desc.as_ref().unwrap().id;
    let mut group_client = GroupClient::new(migrate_meta.src_group_id, router.clone());
    while let Ok(()) = replica.on_leader(true).await {
        match pull_shard_round(&mut group_client, replica.as_ref(), &migrate_meta).await {
            Ok(()) => {
                // TODO(walter)
                // update migrate state to half finished.
                return;
            }
            Err(err) => {
                warn!(
                    "replica {} pull shard {}: {}",
                    info.replica_id, shard_id, err
                );
            }
        }
    }
}

async fn pull_shard_round(
    group_client: &mut GroupClient,
    replica: &Replica,
    migrate_meta: &MigrateMeta,
) -> Result<()> {
    let shard_id = migrate_meta.shard_desc.as_ref().unwrap().id;
    let mut shard_chunk_stream = group_client
        .pull(shard_id, &migrate_meta.last_migrated_key)
        .await?;
    while let Some(shard_chunk) = shard_chunk_stream.next().await {
        let shard_chunk = shard_chunk?;
        replica.ingest(shard_id, shard_chunk).await?;
    }
    Ok(())
}

pub struct ShardChunkStream {
    shard_id: u64,
    last_key: Vec<u8>,
    replica: Arc<Replica>,
}

impl ShardChunkStream {
    pub fn new(shard_id: u64, last_key: Vec<u8>, replica: Arc<Replica>) -> Self {
        ShardChunkStream {
            shard_id,
            last_key,
            replica,
        }
    }

    async fn next_shard_chunk(&mut self) -> Result<Option<ShardChunk>> {
        let shard_chunk = self
            .replica
            .fetch_shard_chunk(self.shard_id, &self.last_key)
            .await?;
        if shard_chunk.data.is_empty() {
            Ok(None)
        } else {
            Ok(Some(shard_chunk))
        }
    }
}

impl futures::Stream for ShardChunkStream {
    type Item = std::result::Result<ShardChunk, tonic::Status>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let future = self.get_mut().next_shard_chunk();
        futures::pin_mut!(future);
        match future.poll(cx) {
            Poll::Ready(Ok(chunk)) => Poll::Ready(chunk.map(Ok)),
            Poll::Ready(Err(err)) => Poll::Ready(Some(Err(err.into()))),
            Poll::Pending => Poll::Pending,
        }
    }
}
