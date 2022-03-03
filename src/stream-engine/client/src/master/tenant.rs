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

use super::{client::MasterClient, stream::Stream};
use crate::{Error, Result};

#[derive(Clone)]
pub struct Tenant {
    inner: Arc<TenantInner>,
}

impl Tenant {
    pub fn new(name: String, master_client: MasterClient) -> Self {
        let inner = TenantInner {
            name,
            master_client,
        };
        Self {
            inner: Arc::new(inner),
        }
    }

    pub async fn desc(&self) -> Result<TenantDesc> {
        let req = DescribeTenantRequest {
            name: self.inner.name.clone(),
        };
        let req = tenant_request_union::Request::DescribeTenant(req);
        let res = self.inner.tenant_union_call(req).await?;
        let desc = if let tenant_response_union::Response::DescribeTenant(res) = res {
            res.desc
        } else {
            None
        };
        desc.ok_or(Error::InvalidResponse)
    }

    pub async fn stream(&self, name: &str) -> Result<Stream> {
        let req = DescribeStreamRequest {
            name: name.to_owned(),
        };
        let req = stream_request_union::Request::DescribeStream(req);
        let res = self.inner.stream_union_call(req).await?;
        let desc = if let stream_response_union::Response::DescribeStream(res) = res {
            res.desc
        } else {
            None
        };
        let stream_desc = desc.ok_or(Error::InvalidResponse)?;
        Ok(self.inner.new_stream_client(stream_desc))
    }

    pub async fn create_stream(&self, name: &str) -> Result<Stream> {
        self.create_stream_client(name).await
    }

    pub async fn delete_stream(&self, name: &str) -> Result<()> {
        let req = DeleteStreamRequest {
            name: name.to_owned(),
        };
        let req = stream_request_union::Request::DeleteStream(req);
        self.inner.stream_union_call(req).await?;
        Ok(())
    }

    pub(super) async fn create_stream_client(&self, name: &str) -> Result<Stream> {
        let desc = StreamDesc {
            name: name.to_owned(),
            ..Default::default()
        };
        let req = CreateStreamRequest { desc: Some(desc) };
        let req = stream_request_union::Request::CreateStream(req);
        let res = self.inner.stream_union_call(req).await?;
        let desc = if let stream_response_union::Response::CreateStream(res) = res {
            res.desc
        } else {
            None
        };
        let stream_desc = desc.ok_or(Error::InvalidResponse)?;
        Ok(self.inner.new_stream_client(stream_desc))
    }
}

struct TenantInner {
    name: String,
    master_client: MasterClient,
}

impl TenantInner {
    fn new_stream_client(&self, stream_desc: StreamDesc) -> Stream {
        Stream::new(self.name.clone(), stream_desc, self.master_client.clone())
    }

    async fn tenant_union_call(
        &self,
        req: tenant_request_union::Request,
    ) -> Result<tenant_response_union::Response> {
        self.master_client.tenant_union(req).await
    }

    async fn stream_union_call(
        &self,
        req: stream_request_union::Request,
    ) -> Result<stream_response_union::Response> {
        self.master_client
            .stream_union(self.name.clone(), req)
            .await
    }
}

#[cfg(test)]
mod tests {
    use stream_engine_master::build_master;

    use crate::{master::Master, Error, Result};

    #[tokio::test(flavor = "multi_thread")]
    async fn create_tenant() -> Result<()> {
        let master_addr = build_master(&[]).await?;

        let master = Master::new(&master_addr).await?;
        master.create_tenant("tenant").await?;
        match master.create_tenant("tenant").await {
            Err(Error::AlreadyExists(_)) => {}
            _ => panic!("create same tenant must fail"),
        }
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn create_stream() -> Result<()> {
        let master_addr = build_master(&[]).await?;

        let master = Master::new(&master_addr).await?;
        master.create_tenant("tenant").await?;

        let tenant = master.tenant("tenant");
        match tenant.stream("stream").await {
            Err(Error::NotFound(_)) => {}
            _ => panic!("no such stream exists"),
        }

        tenant.create_stream_client("stream").await?;
        match tenant.create_stream_client("stream").await {
            Err(Error::AlreadyExists(_)) => {}
            _ => panic!("create same stream must fail"),
        }

        Ok(())
    }
}
