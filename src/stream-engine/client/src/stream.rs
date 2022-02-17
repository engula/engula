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

use std::sync::Arc;

use stream_engine_proto::*;

use crate::{master::Master, Error, Result};

#[derive(Clone)]
pub struct Stream {
    inner: Arc<StreamInner>,
}

impl Stream {
    pub(crate) fn new(stream: String, tenant: String, master: Master) -> Self {
        let inner = StreamInner {
            tenant,
            stream,
            master,
        };
        Self {
            inner: Arc::new(inner),
        }
    }

    pub async fn desc(&self) -> Result<StreamDesc> {
        let req = DescribeStreamRequest {
            name: self.inner.stream.clone(),
        };
        let req = stream_request_union::Request::DescribeStream(req);
        let res = self.inner.stream_union_call(req).await?;
        let desc = if let stream_response_union::Response::DescribeStream(res) = res {
            res.desc
        } else {
            None
        };
        desc.ok_or(Error::InvalidResponse)
    }
}

struct StreamInner {
    tenant: String,
    stream: String,
    master: Master,
}

impl StreamInner {
    async fn stream_union_call(
        &self,
        req: stream_request_union::Request,
    ) -> Result<stream_response_union::Response> {
        self.master.stream_union(self.tenant.clone(), req).await
    }
}
