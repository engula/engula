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
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use engula_api::server::v1::*;
use engula_client::MigrateClient;
use futures::StreamExt;
use tracing::warn;

use crate::{
    node::{metrics::*, Replica},
    record_latency, Result,
};

pub async fn pull_shard(
    client: &mut MigrateClient,
    replica: &Replica,
    desc: &MigrationDesc,
    last_migrated_key: Vec<u8>,
) -> Result<()> {
    record_latency!(take_pull_shard_metrics());
    let shard_id = desc.get_shard_id();
    let mut streaming = client.retryable_pull(shard_id, last_migrated_key).await?;
    while let Some(shard_chunk) = streaming.next().await {
        let shard_chunk = shard_chunk?;
        NODE_INGEST_CHUNK_TOTAL.inc();
        replica.ingest(shard_id, shard_chunk, false).await?;
    }
    Ok(())
}

fn shard_chunk_stream(
    shard_id: u64,
    chunk_size: usize,
    mut last_key: Vec<u8>,
    replica: Arc<Replica>,
) -> impl futures::Stream<Item = Result<ShardChunk, tonic::Status>> {
    async_stream::try_stream! {
        loop {
            match replica
                .fetch_shard_chunk(shard_id, &last_key, chunk_size)
                .await
            {
                Ok(shard_chunk) => {
                    if shard_chunk.data.is_empty() {
                        break;
                    }

                    last_key = shard_chunk.data.last().as_ref().unwrap().key.clone();
                    yield shard_chunk;
                },
                Err(e) => {
                    warn!("shard {shard_id} fetch shard chunk: {e:?}");
                    Err(e)?;
                    unreachable!();
                }
            }
        }
    }
}

type ShardChunkResult = Result<ShardChunk, tonic::Status>;

pub struct ShardChunkStream {
    inner: Pin<Box<dyn futures::Stream<Item = ShardChunkResult> + Send + 'static>>,
}

impl ShardChunkStream {
    pub fn new(shard_id: u64, chunk_size: usize, last_key: Vec<u8>, replica: Arc<Replica>) -> Self {
        let inner = shard_chunk_stream(shard_id, chunk_size, last_key, replica);
        ShardChunkStream {
            inner: Box::pin(inner),
        }
    }
}

impl futures::Stream for ShardChunkStream {
    type Item = Result<ShardChunk, tonic::Status>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let me = self.get_mut();
        Pin::new(&mut me.inner).poll_next(cx)
    }
}
