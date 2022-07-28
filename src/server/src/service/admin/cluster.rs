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

use serde_json::json;
use tonic::{async_trait, codegen::http};

use crate::{Result, Server};

pub(super) struct CordonHandle {
    server: Server,
}

impl CordonHandle {
    pub(crate) fn new(server: Server) -> Self {
        Self { server }
    }
}

#[async_trait]
impl super::service::HttpHandle for CordonHandle {
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
        self.server.root.cordon_node(node_id).await?;
        Ok(http::Response::builder()
            .status(http::StatusCode::OK)
            .body("".to_owned())
            .unwrap())
    }
}

pub(super) struct UncordonHandle {
    server: Server,
}

impl UncordonHandle {
    pub(crate) fn new(server: Server) -> Self {
        Self { server }
    }
}

#[async_trait]
impl super::service::HttpHandle for UncordonHandle {
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
        self.server.root.uncordon_node(node_id).await?;
        Ok(http::Response::builder()
            .status(http::StatusCode::OK)
            .body("".to_owned())
            .unwrap())
    }
}

pub(super) struct DrainHandle {
    server: Server,
}

impl DrainHandle {
    pub(crate) fn new(server: Server) -> Self {
        Self { server }
    }
}

#[async_trait]
impl super::service::HttpHandle for DrainHandle {
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
        self.server.root.begin_drain(node_id).await?;
        Ok(http::Response::builder()
            .status(http::StatusCode::OK)
            .body("".to_owned())
            .unwrap())
    }
}

pub(super) struct StatusHandle {
    server: Server,
}

impl StatusHandle {
    pub(crate) fn new(server: Server) -> Self {
        Self { server }
    }
}

#[async_trait]
impl super::service::HttpHandle for StatusHandle {
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
        let status = self.server.root.node_status(node_id).await?;
        Ok(http::Response::builder()
            .status(http::StatusCode::OK)
            .body(json!({ "node_id": node_id, "node_status": status as i32 }).to_string())
            .unwrap())
    }
}
