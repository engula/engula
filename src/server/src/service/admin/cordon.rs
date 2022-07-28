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

use std::collections::HashMap;

use tonic::{async_trait, codegen::http};

use crate::{Result, Server};

pub(super) struct NodeCordonHandle {
    server: Server,
}

impl NodeCordonHandle {
    pub(crate) fn new(server: Server) -> Self {
        Self { server }
    }
}

#[async_trait]
impl super::service::HttpHandle for NodeCordonHandle {
    async fn call(
        &self,
        _: &str,
        params: &HashMap<String, String>,
    ) -> Result<http::Response<String>> {
        let node_id = params
            .get("node_id")
            .ok_or_else(|| crate::Error::InvalidArgument("node_id is required".into()))?
            .parse::<u64>()
            .map_err(|_| crate::Error::InvalidArgument("illegal node_id".into()))?;
        self.server.root.evit(node_id).await?;
        Ok(http::Response::builder()
            .status(http::StatusCode::OK)
            .body("".to_owned())
            .unwrap())
    }
}

pub(super) struct NodeUncordonHandle {
    server: Server,
}

impl NodeUncordonHandle {
    pub(crate) fn new(server: Server) -> Self {
        Self { server }
    }
}

#[async_trait]
impl super::service::HttpHandle for NodeUncordonHandle {
    async fn call(
        &self,
        _: &str,
        params: &HashMap<String, String>,
    ) -> Result<http::Response<String>> {
        let node_id = params
            .get("node_id")
            .ok_or_else(|| crate::Error::InvalidArgument("node_id is required".into()))?
            .parse::<u64>()
            .map_err(|_| crate::Error::InvalidArgument("illegal node_id".into()))?;
        self.server.root.evit(node_id).await?;
        Ok(http::Response::builder()
            .status(http::StatusCode::OK)
            .body("".to_owned())
            .unwrap())
    }
}
