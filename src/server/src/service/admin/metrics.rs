use std::collections::HashMap;

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
use lazy_static::lazy_static;
use prometheus::{self, register_int_counter, IntCounter, TextEncoder};
use tonic::codegen::*;

use crate::{
    root::{RootCollector, RootCollectorShared},
    Server,
};

lazy_static! {
    // A special metric for testing metrics pulling.
    pub static ref METRICS_RPC_REQUESTS_TOTAL: IntCounter = register_int_counter!(
        "metrics_rpc_requests_total",
        "Number of QPS for /admin/metrics",
    )
    .unwrap();
}

pub(super) struct MetricsHandle {
    collector_shard: Arc<RootCollectorShared>,
}

impl MetricsHandle {
    pub fn new(server: Server) -> Self {
        let collector_shard = Arc::new(RootCollectorShared::new("", server));
        match &prometheus::register(Box::new(RootCollector::new(collector_shard.to_owned()))) {
            Err(err) if matches!(err, prometheus::Error::AlreadyReg) => {}
            r => {
                r.as_ref().unwrap();
            }
        }
        Self { collector_shard }
    }
}

#[crate::async_trait]
impl super::service::HttpHandle for MetricsHandle {
    async fn call(
        &self,
        _: &str,
        _: &HashMap<String, String>,
    ) -> crate::Result<http::Response<String>> {
        METRICS_RPC_REQUESTS_TOTAL.inc();
        self.collector_shard.try_refresh().await;
        let encoder = TextEncoder::new();
        let metric_families = prometheus::gather();
        let content = encoder
            .encode_to_string(&metric_families)
            .map_err(|e| crate::Error::InvalidData(e.to_string()))?;

        Ok(http::Response::builder()
            .status(http::StatusCode::OK)
            .body(content)
            .unwrap())
    }
}
