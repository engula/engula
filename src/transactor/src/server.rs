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

use engula_apis::*;
use tonic::{Request, Response, Status};

use crate::Transactor;

type TonicResult<T> = std::result::Result<T, Status>;

pub struct Server {
    inner: Transactor,
}

impl Default for Server {
    fn default() -> Self {
        Self::new()
    }
}

impl Server {
    pub fn new() -> Self {
        Self {
            inner: Transactor::new(),
        }
    }

    pub fn into_service(self) -> txn_server::TxnServer<Self> {
        txn_server::TxnServer::new(self)
    }
}

#[tonic::async_trait]
impl txn_server::Txn for Server {
    async fn batch(
        &self,
        req: Request<BatchTxnRequest>,
    ) -> TonicResult<Response<BatchTxnResponse>> {
        let req = req.into_inner();
        let res = self.inner.execute(req).await?;
        Ok(Response::new(res))
    }
}
