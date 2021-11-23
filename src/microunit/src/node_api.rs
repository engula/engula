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

use crate::{
    error::Result,
    node::Node,
    proto::{UnitDesc, UnitDescList, UnitSpec},
};

pub fn route(node: Arc<Node>) -> Router {
    let v1 = Router::new().route("/units", get(list_units).post(create_unit));
    Router::new()
        .nest("/v1", v1)
        .layer(AddExtensionLayer::new(node))
}

async fn list_units(Extension(node): Extension<Arc<Node>>) -> Result<Json<UnitDescList>> {
    let descs = node.list_units().await?;
    Ok(descs.into())
}

async fn create_unit(
    Extension(node): Extension<Arc<Node>>,
    Json(spec): Json<UnitSpec>,
) -> Result<Json<UnitDesc>> {
    let desc = node.create_unit(spec).await?;
    Ok(desc.into())
}
