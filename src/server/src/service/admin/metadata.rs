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

use tonic::codegen::*;

use crate::Server;

pub(super) struct MetadataHandle {
    server: Server,
}

impl MetadataHandle {
    pub fn new(server: Server) -> Self {
        Self { server }
    }
}

#[crate::async_trait]
impl super::service::HttpHandle for MetadataHandle {
    async fn call(&self, path: &str) -> crate::Result<http::Response<String>> {
        let info = match self.server.root.info().await {
            Ok(info) => info,
            Err(e @ crate::Error::NotRootLeader(..)) => {
                let root_desc = self.server.node.get_root().await;
                let node = root_desc.root_nodes.get(0);
                if node.is_none() {
                    return Err(e);
                }
                let resp = http::Response::builder()
                    .status(http::StatusCode::PERMANENT_REDIRECT)
                    .header(
                        http::header::LOCATION,
                        format!("http://{}{}", node.unwrap().addr, path),
                    )
                    .body("".into())
                    .unwrap();
                return Ok(resp);
            }
            Err(e) => return Err(e),
        };
        Ok(http::Response::builder()
            .status(http::StatusCode::OK)
            .body(info)
            .unwrap())
    }
}
