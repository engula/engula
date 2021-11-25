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

use std::sync::Arc;

use axum::{extract::Extension, routing::get, AddExtensionLayer, Json, Router};

use crate::{control::Control, error::Result, proto::*};

pub fn route(ctrl: Arc<Control>) -> Router {
    let universe = Router::new().route("/nodes", get(list_nodes));
    let v1 = Router::new()
        .route("/", get(desc))
        .nest("/universe", universe);
    Router::new()
        .nest("/v1", v1)
        .layer(AddExtensionLayer::new(ctrl))
}

async fn desc(Extension(ctrl): Extension<Arc<Control>>) -> Result<Json<ControlDesc>> {
    let desc = ctrl.desc().await;
    Ok(desc.into())
}

async fn list_nodes(Extension(ctrl): Extension<Arc<Control>>) -> Result<Json<NodeDescList>> {
    let list = ctrl.list_nodes().await?;
    Ok(list.into())
}
