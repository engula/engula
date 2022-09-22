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

use serde::Serialize;
use tonic::codegen::*;

use crate::{
    node::replica::ReplicaPerfContext, raftgroup::perf_point_micros, runtime::TaskPriority, Error,
    Result, Server,
};

#[derive(Default, Debug, Clone, Serialize)]
pub struct PerfContext {
    pub start: u64,
    pub end: u64,
    pub replica: ReplicaPerfContext,
}

pub(super) struct MonitorHandle {
    server: Server,
}

impl MonitorHandle {
    pub(crate) fn new(server: Server) -> Self {
        Self { server }
    }
}

#[async_trait]
impl super::service::HttpHandle for MonitorHandle {
    async fn call(
        &self,
        _: &str,
        params: &HashMap<String, String>,
    ) -> Result<http::Response<String>> {
        let group_id = params
            .get("group_id")
            .ok_or_else(|| Error::InvalidArgument("group_id is required".into()))?
            .parse::<u64>()
            .map_err(|_| Error::InvalidArgument("illegal group_id".into()))?;

        let replica = self
            .server
            .node
            .replica_table()
            .find(group_id)
            .ok_or(Error::GroupNotFound(group_id))?;

        let start = perf_point_micros();

        // We also need to record the delay from task spawn to execute.
        let replica_perf_ctx = self
            .server
            .node
            .executor()
            .dispatch(
                None,
                TaskPriority::Low,
                async move { replica.monitor().await },
            )
            .await?;

        let monitor = PerfContext {
            start,
            replica: replica_perf_ctx,
            end: perf_point_micros(),
        };
        Ok(http::Response::builder()
            .status(http::StatusCode::OK)
            .body(serde_json::to_string(&monitor).unwrap_or_else(|e| e.to_string()))
            .unwrap())
    }
}
