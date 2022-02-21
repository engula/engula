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

use stream_engine_proto::*;
use tonic::{Request, Response, Status};

use crate::{
    error::{Error, Result},
    master::{Config, Master, Tenant},
    stream::StreamInfo,
};

type TonicResult<T> = std::result::Result<T, Status>;

pub struct Server {
    master: Master,
}

impl Default for Server {
    fn default() -> Self {
        Self::new()
    }
}

impl Server {
    pub fn new() -> Self {
        // FIXME(w41ter) add store addresses.
        let stores = vec![];
        Self {
            master: Master::new(Config::default(), stores),
        }
    }

    pub fn into_service(self) -> master_server::MasterServer<Self> {
        master_server::MasterServer::new(self)
    }
}

#[tonic::async_trait]
impl master_server::Master for Server {
    async fn tenant(&self, req: Request<TenantRequest>) -> TonicResult<Response<TenantResponse>> {
        let req = req.into_inner();
        let res = self.handle_tenant(req).await?;
        Ok(Response::new(res))
    }

    async fn stream(&self, req: Request<StreamRequest>) -> TonicResult<Response<StreamResponse>> {
        let req = req.into_inner();
        let res = self.handle_stream(req).await?;
        Ok(Response::new(res))
    }

    async fn segment(
        &self,
        req: Request<SegmentRequest>,
    ) -> TonicResult<Response<SegmentResponse>> {
        let req = req.into_inner();
        let res = self.handle_segment(req).await?;
        Ok(Response::new(res))
    }

    async fn heartbeat(
        &self,
        req: Request<HeartbeatRequest>,
    ) -> TonicResult<Response<HeartbeatResponse>> {
        let req = req.into_inner();
        let tenant = self.master.tenant(&req.tenant).await?;
        let stream = tenant.stream(req.stream_id).await?;

        let commands = stream
            .heartbeat(
                &self.master.config,
                &self.master.stores,
                &req.observer_id,
                req.observer_state.into(),
                req.role.into(),
                req.writer_epoch,
                req.acked_seq,
            )
            .await?;

        Ok(Response::new(HeartbeatResponse { commands }))
    }
}

impl Server {
    pub(crate) async fn handle_tenant(&self, req: TenantRequest) -> Result<TenantResponse> {
        let mut res = TenantResponse::default();
        for req_union in req.requests {
            let res_union = self.handle_tenant_union(req_union).await?;
            res.responses.push(res_union);
        }
        Ok(res)
    }

    pub(crate) async fn handle_tenant_union(
        &self,
        req: TenantRequestUnion,
    ) -> Result<TenantResponseUnion> {
        type Request = tenant_request_union::Request;
        type Response = tenant_response_union::Response;

        let req = req.request.ok_or(Error::InvalidRequest)?;
        let res = match req {
            Request::ListTenants(_req) => {
                todo!();
            }
            Request::CreateTenant(req) => {
                let res = self.handle_create_tenant(req).await?;
                Response::CreateTenant(res)
            }
            Request::UpdateTenant(_req) => {
                todo!();
            }
            Request::DeleteTenant(_req) => {
                todo!();
            }
            Request::DescribeTenant(req) => {
                let res = self.handle_describe_tenant(req).await?;
                Response::DescribeTenant(res)
            }
        };
        Ok(TenantResponseUnion {
            response: Some(res),
        })
    }

    async fn handle_create_tenant(&self, req: CreateTenantRequest) -> Result<CreateTenantResponse> {
        let desc = req.desc.ok_or(Error::InvalidRequest)?;
        let desc = self.master.create_tenant(desc).await?;
        Ok(CreateTenantResponse { desc: Some(desc) })
    }

    async fn handle_describe_tenant(
        &self,
        req: DescribeTenantRequest,
    ) -> Result<DescribeTenantResponse> {
        let db = self.master.tenant(&req.name).await?;
        let desc = db.desc().await;
        Ok(DescribeTenantResponse { desc: Some(desc) })
    }
}

impl Server {
    async fn handle_stream(&self, req: StreamRequest) -> Result<StreamResponse> {
        let tenant = self.master.tenant(&req.tenant).await?;
        let mut res = StreamResponse::default();
        for req_union in req.requests {
            let res_union = self.handle_stream_union(tenant.clone(), req_union).await?;
            res.responses.push(res_union);
        }
        Ok(res)
    }

    async fn handle_stream_union(
        &self,
        tenant: Tenant,
        req: StreamRequestUnion,
    ) -> Result<StreamResponseUnion> {
        type Request = stream_request_union::Request;
        type Response = stream_response_union::Response;

        let req = req.request.ok_or(Error::InvalidRequest)?;
        let res = match req {
            Request::ListStreams(_req) => {
                todo!();
            }
            Request::CreateStream(req) => {
                let res = self.handle_create_stream(tenant, req).await?;
                Response::CreateStream(res)
            }
            Request::UpdateStream(_req) => {
                todo!();
            }
            Request::DeleteStream(_req) => {
                todo!();
            }
            Request::DescribeStream(req) => {
                let res = self.handle_describe_stream(tenant, req).await?;
                Response::DescribeStream(res)
            }
        };
        Ok(StreamResponseUnion {
            response: Some(res),
        })
    }

    async fn handle_create_stream(
        &self,
        tenant: Tenant,
        req: CreateStreamRequest,
    ) -> Result<CreateStreamResponse> {
        let desc = req.desc.ok_or(Error::InvalidRequest)?;
        let desc = tenant.create_stream(desc).await?;
        Ok(CreateStreamResponse { desc: Some(desc) })
    }

    async fn handle_describe_stream(
        &self,
        tenant: Tenant,
        req: DescribeStreamRequest,
    ) -> Result<DescribeStreamResponse> {
        let desc = tenant.stream_desc(&req.name).await?;
        Ok(DescribeStreamResponse { desc: Some(desc) })
    }
}

impl Server {
    async fn handle_segment(&self, req: SegmentRequest) -> Result<SegmentResponse> {
        let tenant = self.master.tenant(&req.tenant).await?;
        let stream = tenant.stream(req.stream_id).await?;

        let mut res = SegmentResponse::default();
        for req_union in req.requests {
            let res_union = self
                .handle_segment_union(&stream, req.segment_epoch, req_union)
                .await?;
            res.responses.push(res_union);
        }
        Ok(res)
    }

    async fn handle_segment_union(
        &self,
        stream: &StreamInfo,
        segment_epoch: u32,
        req: SegmentRequestUnion,
    ) -> Result<SegmentResponseUnion> {
        type Request = segment_request_union::Request;
        type Response = segment_response_union::Response;

        let res = match req.request.ok_or(Error::InvalidRequest)? {
            Request::GetSegment(req) => {
                Response::GetSegment(self.handle_get_segment(stream, segment_epoch, req).await?)
            }
            Request::SealSegment(req) => {
                Response::SealSegment(self.handle_seal_segment(stream, segment_epoch, req).await?)
            }
        };
        Ok(SegmentResponseUnion {
            response: Some(res),
        })
    }

    async fn handle_get_segment(
        &self,
        stream: &StreamInfo,
        segment_epoch: u32,
        _: GetSegmentRequest,
    ) -> Result<GetSegmentResponse> {
        let segment = stream.segment(segment_epoch).await?;
        Ok(GetSegmentResponse {
            desc: Some(segment),
        })
    }

    async fn handle_seal_segment(
        &self,
        stream: &StreamInfo,
        segment_epoch: u32,
        _: SealSegmentRequest,
    ) -> Result<SealSegmentResponse> {
        stream.seal(segment_epoch).await?;
        Ok(SealSegmentResponse {})
    }
}
