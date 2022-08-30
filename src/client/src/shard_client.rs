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

use engula_api::{
    server::v1::{group_request_union::Request, group_response_union::Response, *},
    v1::DeleteRequest,
};

use crate::{ConnManager, Error, GroupClient, Result, RetryState, Router};

/// `ShardClient` wraps `GroupClient` and provides retry for shard-related functions.
///
/// Since it will retry all requests in the current group, the user must ensure that the shard will
/// not be migrated during the request process.
pub struct ShardClient {
    group_id: u64,
    shard_id: u64,
    router: Router,
    conn_manager: ConnManager,
}

impl ShardClient {
    pub fn new(group_id: u64, shard_id: u64, router: Router, conn_manager: ConnManager) -> Self {
        ShardClient {
            group_id,
            shard_id,
            router,
            conn_manager,
        }
    }

    pub async fn prefix_list(&self, prefix: &[u8]) -> Result<Vec<Vec<u8>>> {
        let mut retry_state = RetryState::new(None);

        loop {
            match self.prefix_list_inner(prefix).await {
                Ok(value) => return Ok(value),
                Err(err) => {
                    retry_state.retry(err).await?;
                }
            }
        }
    }

    pub async fn delete(&self, key: &[u8]) -> Result<()> {
        let mut retry_state = RetryState::new(None);

        loop {
            match self.delete_inner(key).await {
                Ok(_) => return Ok(()),
                Err(err) => {
                    retry_state.retry(err).await?;
                }
            }
        }
    }

    async fn prefix_list_inner(&self, prefix: &[u8]) -> Result<Vec<Vec<u8>>> {
        let req = Request::PrefixList(ShardPrefixListRequest {
            shard_id: self.shard_id,
            prefix: prefix.to_owned(),
        });
        let mut client = GroupClient::lazy(
            self.group_id,
            self.router.clone(),
            self.conn_manager.clone(),
        );
        match client.request(&req).await? {
            Response::PrefixList(ShardPrefixListResponse { values }) => Ok(values),
            _ => Err(Error::Internal(
                "invalid response type, `SharedPrefixListResponse` is required".into(),
            )),
        }
    }

    async fn delete_inner(&self, key: &[u8]) -> Result<()> {
        let req = Request::Delete(ShardDeleteRequest {
            shard_id: self.shard_id,
            delete: Some(DeleteRequest {
                key: key.to_owned(),
            }),
        });
        let mut client = GroupClient::lazy(
            self.group_id,
            self.router.clone(),
            self.conn_manager.clone(),
        );
        client.request(&req).await?;
        Ok(())
    }
}
