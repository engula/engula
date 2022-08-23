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

use crate::{
    ConnManager, Error, GroupClient, Result, RetryState, RetryableShardChunkStreaming, Router,
};

/// `MigrateClient` wraps `GroupClient` and provides retry for migration-related functions.
pub struct MigrateClient {
    group_id: u64,
    router: Router,
    conn_manager: ConnManager,
}

impl MigrateClient {
    pub fn new(group_id: u64, router: Router, conn_manager: ConnManager) -> Self {
        MigrateClient {
            group_id,
            router,
            conn_manager,
        }
    }
    pub async fn setup_migration(&mut self, desc: &MigrationDesc) -> Result<MigrateResponse> {
        let mut retry_state = RetryState::new(None);

        loop {
            let mut client = self.group_client();
            match client.setup_migration(desc).await {
                Ok(resp) => return Ok(resp),
                e @ Err(Error::EpochNotMatch(..)) => return e,
                Err(err) => {
                    retry_state.retry(err).await?;
                }
            }
        }
    }

    pub async fn commit_migration(&mut self, desc: &MigrationDesc) -> Result<MigrateResponse> {
        let mut retry_state = RetryState::new(None);

        loop {
            let mut client = self.group_client();
            match client.commit_migration(desc).await {
                Ok(resp) => return Ok(resp),
                Err(err) => {
                    retry_state.retry(err).await?;
                }
            }
        }
    }

    pub async fn retryable_pull(
        &mut self,
        shard_id: u64,
        last_key: Vec<u8>,
    ) -> Result<RetryableShardChunkStreaming> {
        let mut retry_state = RetryState::new(None);

        loop {
            let client = self.group_client();
            match client.retryable_pull(shard_id, last_key.clone()).await {
                Ok(resp) => return Ok(resp),
                Err(err) => {
                    retry_state.retry(err).await?;
                }
            }
        }
    }

    pub async fn forward(&mut self, req: &ForwardRequest) -> Result<ForwardResponse> {
        let mut retry_state = RetryState::new(None);

        loop {
            let mut client = self.group_client();
            match client.forward(req).await {
                Ok(resp) => return Ok(resp),
                Err(err) => {
                    retry_state.retry(err).await?;
                }
            }
        }
    }

    #[inline]
    fn group_client(&self) -> GroupClient {
        GroupClient::new(
            self.group_id,
            self.router.clone(),
            self.conn_manager.clone(),
        )
    }
}
