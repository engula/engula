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

use std::{collections::HashMap, sync::Arc};

use stream_engine_proto::{ReadRequest, ReadResponse, SealRequest, WriteRequest};
use tokio::sync::Mutex;
use tonic::{transport::Endpoint, Streaming};

use super::client::StoreClient;
use crate::Result;

#[derive(Clone)]
pub struct Transport {
    inner: Arc<Mutex<TransportInner>>,
}

struct TransportInner {
    clients: HashMap<String, StoreClient>,
    stream_set: HashMap<u64, ()>,
}

#[allow(dead_code)]
impl Transport {
    pub fn new() -> Self {
        Transport {
            inner: Arc::new(Mutex::new(TransportInner {
                clients: HashMap::new(),
                stream_set: HashMap::new(),
            })),
        }
    }

    pub async fn register(&self, stream_id: u64) {
        let mut inner = self.inner.lock().await;
        inner.stream_set.insert(stream_id, ());
    }

    pub async fn unregister(&self, stream_id: u64) {
        let mut inner = self.inner.lock().await;
        inner.stream_set.remove(&stream_id);
    }

    pub async fn read(
        &self,
        target: String,
        stream_id: u64,
        seg_epoch: u32,
        start_index: u32,
        only_include_acked: bool,
    ) -> Result<Streaming<ReadResponse>> {
        let client = self.get_client(target).await?;
        let req = ReadRequest {
            stream_id,
            seg_epoch,
            start_index,
            limit: u32::MAX,
            include_pending_entries: !only_include_acked,
        };
        client.read(req).await
    }

    pub async fn write(
        &self,
        target: String,
        stream_id: u64,
        writer_epoch: u32,
        write: WriteRequest,
    ) -> Result<(u32, u32)> {
        let client = self.get_client(target).await?;
        let resp = client.write(stream_id, writer_epoch, write).await?;
        Ok((resp.matched_index, resp.acked_index))
    }

    pub async fn seal(
        &self,
        target: String,
        stream_id: u64,
        writer_epoch: u32,
        segment_epoch: u32,
    ) -> Result<u32> {
        let client = self.get_client(target).await?;
        let resp = client
            .seal(stream_id, writer_epoch, SealRequest { segment_epoch })
            .await?;
        Ok(resp.acked_index)
    }

    async fn get_client(&self, target: String) -> Result<StoreClient> {
        let inner = self.inner.lock().await;
        if let Some(client) = inner.clients.get(&target) {
            Ok(client.clone())
        } else {
            drop(inner);

            // FIXME(w41ter) too many concurrent connections.
            let channel = Endpoint::new(target.clone())?.connect().await?;
            let client = StoreClient::new(channel);
            let mut inner = self.inner.lock().await;
            inner.clients.insert(target, client.clone());
            Ok(client)
        }
    }
}
