// Copyright 2021 The Engula Authors.
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

use std::{net::SocketAddr, sync::Arc};

use axum::{Router, Server};

use super::{error::Result, node::Node};

/// An HTTP server that serves a node.
pub struct NodeServer {
    node: Arc<Node>,
}

impl NodeServer {
    pub fn new(node: Node) -> NodeServer {
        NodeServer {
            node: Arc::new(node),
        }
    }

    pub async fn bind(&self, addr: SocketAddr) -> Result<()> {
        let v1 = v1::route(self.node.clone());
        let router = Router::new().nest("/v1", v1);
        Server::bind(&addr)
            .serve(router.into_make_service())
            .await?;
        Ok(())
    }
}

mod v1 {
    use std::sync::Arc;

    use axum::{
        extract::Extension, http::StatusCode, response::IntoResponse, routing::get,
        AddExtensionLayer, Json, Router,
    };
    use serde_json::json;

    use crate::{node::Node, unit::UnitSpec};

    pub fn route(node: Arc<Node>) -> Router {
        Router::new()
            .route("/status", get(status))
            .route("/units", get(list_units).post(create_unit))
            .layer(AddExtensionLayer::new(node))
    }

    async fn status(Extension(node): Extension<Arc<Node>>) -> impl IntoResponse {
        let desc = node.status();
        (StatusCode::OK, Json(desc))
    }

    async fn list_units(Extension(node): Extension<Arc<Node>>) -> impl IntoResponse {
        let descs = node.list_units().await;
        (StatusCode::OK, Json(descs))
    }

    async fn create_unit(
        Json(spec): Json<UnitSpec>,
        Extension(node): Extension<Arc<Node>>,
    ) -> impl IntoResponse {
        match node.create_unit(spec).await {
            Ok(desc) => {
                let resp = json!({
                    "desc": desc,
                });
                (StatusCode::CREATED, Json(resp))
            }
            Err(err) => {
                let resp = json!({
                    "error": err.to_string(),
                });
                (StatusCode::INTERNAL_SERVER_ERROR, Json(resp))
            }
        }
    }
}
