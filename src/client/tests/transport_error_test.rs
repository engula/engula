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

use std::{
    error::Error,
    io::ErrorKind,
    net::SocketAddr,
    os::unix::prelude::{FromRawFd, IntoRawFd},
    panic,
    pin::Pin,
    process,
    task::{Context, Poll},
    time::Duration,
};

use engula_api::server::v1::{node_server::NodeServer, *};
use engula_client::{
    error::{find_io_error, retryable_rpc_err, transport_err},
    NodeClient, RequestBatchBuilder,
};
use socket2::{Domain, Socket, Type};
use tokio::sync::oneshot;
use tonic::{transport::Endpoint, Response};
use tracing::info;

pub fn setup_panic_hook() {
    let orig_hook = panic::take_hook();
    panic::set_hook(Box::new(move |panic_info| {
        // invoke the default handler and exit the process
        orig_hook(panic_info);
        tracing::error!("{:#?}", panic_info);
        process::exit(1);
    }));
}
#[ctor::ctor]
fn init() {
    tracing_subscriber::fmt::init();
    setup_panic_hook();
}

struct ShardChunkStream {}

#[allow(unused)]
impl futures::Stream for ShardChunkStream {
    type Item = std::result::Result<ShardChunk, tonic::Status>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        todo!()
    }
}

struct MockedServer {}

#[allow(unused)]
#[tonic::async_trait]
impl node_server::Node for MockedServer {
    type PullStream = ShardChunkStream;

    async fn batch(
        &self,
        request: tonic::Request<engula_api::server::v1::BatchRequest>,
    ) -> Result<tonic::Response<engula_api::server::v1::BatchResponse>, tonic::Status> {
        todo!()
    }

    async fn get_root(
        &self,
        request: tonic::Request<engula_api::server::v1::GetRootRequest>,
    ) -> Result<tonic::Response<engula_api::server::v1::GetRootResponse>, tonic::Status> {
        info!("receive get root");
        Ok(Response::new(GetRootResponse {
            root: Some(RootDesc::default()),
        }))
    }

    async fn create_replica(
        &self,
        request: tonic::Request<engula_api::server::v1::CreateReplicaRequest>,
    ) -> Result<tonic::Response<engula_api::server::v1::CreateReplicaResponse>, tonic::Status> {
        todo!()
    }

    async fn remove_replica(
        &self,
        request: tonic::Request<engula_api::server::v1::RemoveReplicaRequest>,
    ) -> Result<tonic::Response<engula_api::server::v1::RemoveReplicaResponse>, tonic::Status> {
        todo!()
    }

    async fn root_heartbeat(
        &self,
        request: tonic::Request<engula_api::server::v1::HeartbeatRequest>,
    ) -> Result<tonic::Response<engula_api::server::v1::HeartbeatResponse>, tonic::Status> {
        todo!()
    }

    async fn migrate(
        &self,
        request: tonic::Request<engula_api::server::v1::MigrateRequest>,
    ) -> Result<tonic::Response<engula_api::server::v1::MigrateResponse>, tonic::Status> {
        todo!()
    }

    async fn pull(
        &self,
        request: tonic::Request<engula_api::server::v1::PullRequest>,
    ) -> Result<tonic::Response<Self::PullStream>, tonic::Status> {
        todo!()
    }

    async fn forward(
        &self,
        request: tonic::Request<engula_api::server::v1::ForwardRequest>,
    ) -> Result<tonic::Response<engula_api::server::v1::ForwardResponse>, tonic::Status> {
        todo!()
    }
}

#[tokio::test]
async fn broken_pipe() {
    if cfg!(target_os = "macos") {
        return;
    }

    let socket = Socket::new(Domain::IPV4, Type::STREAM, None).unwrap();
    socket.set_linger(Some(Duration::ZERO)).unwrap();
    socket
        .bind(&"127.0.0.1:0".parse::<SocketAddr>().unwrap().into())
        .unwrap();
    socket.listen(1).unwrap();
    let port = socket
        .local_addr()
        .unwrap()
        .as_socket_ipv4()
        .unwrap()
        .port();

    let channel = Endpoint::new(format!("http://127.0.0.1:{port}"))
        .unwrap()
        .connect_timeout(Duration::from_millis(100))
        .connect()
        .await
        .unwrap();
    let client = NodeClient::new(channel);
    let req = RequestBatchBuilder::new(0).transfer_leader(1, 1, 1).build();
    drop(socket);
    match client.batch_group_requests(req).await {
        Ok(_) => unreachable!(),
        Err(status) => {
            info!("message {} details {status:?}", status.message());
            assert!(!retryable_rpc_err(&status));
            assert!(transport_err(&status));
            let err = find_io_error(&status).unwrap();
            assert!(matches!(err.kind(), ErrorKind::BrokenPipe));
        }
    }
}

#[tokio::test]
async fn connection_closed() {
    if cfg!(target_os = "macos") {
        return;
    }

    let socket = Socket::new(Domain::IPV4, Type::STREAM, None).unwrap();
    socket.set_nodelay(true).unwrap();
    socket.set_nonblocking(true).unwrap();
    socket
        .bind(&"127.0.0.1:0".parse::<SocketAddr>().unwrap().into())
        .unwrap();
    socket.listen(10).unwrap();
    let port = socket
        .local_addr()
        .unwrap()
        .as_socket_ipv4()
        .unwrap()
        .port();

    let (sender, receiver) = oneshot::channel::<()>();
    let handle = std::thread::spawn(move || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async move {
                use tokio::net::TcpListener;
                use tokio_stream::wrappers::TcpListenerStream;
                use tonic::transport::Server;

                let listener = unsafe { std::net::TcpListener::from_raw_fd(socket.into_raw_fd()) };
                let listener = TcpListener::from_std(listener).unwrap();
                let listener = TcpListenerStream::new(listener);
                info!("listen mocked service");
                let server = Server::builder()
                    .add_service(NodeServer::new(MockedServer {}))
                    .serve_with_incoming(listener);
                tokio::select! {
                    _ = server => {}
                    _ = receiver => {
                        info!("shutdown");
                    }
                };
            });
    });

    let channel = Endpoint::new(format!("http://127.0.0.1:{port}"))
        .unwrap()
        .connect_timeout(Duration::from_millis(100))
        .connect()
        .await
        .unwrap();

    let client = NodeClient::new(channel);

    drop(sender);
    handle.join().unwrap();
    match client.get_root().await {
        Ok(_) => unreachable!(),
        Err(status) => {
            info!("message {} details {status:?}", status.message());
            assert!(!retryable_rpc_err(&status));
            assert!(transport_err(&status));

            let mut cause = status.source();
            let found = loop {
                if let Some(err) = cause {
                    if err
                        .to_string()
                        .starts_with("operation was canceled: connection closed")
                    {
                        break true;
                    }
                    cause = err.source();
                } else {
                    break false;
                }
            };
            assert!(found);
        }
    }
}

#[tokio::test]
async fn connection_reset() {
    if cfg!(target_os = "macos") {
        return;
    }

    let socket = Socket::new(Domain::IPV4, Type::STREAM, None).unwrap();
    socket.set_linger(Some(Duration::ZERO)).unwrap();
    socket
        .bind(&"127.0.0.1:0".parse::<SocketAddr>().unwrap().into())
        .unwrap();
    socket.listen(100).unwrap();
    let port = socket
        .local_addr()
        .unwrap()
        .as_socket_ipv4()
        .unwrap()
        .port();

    let handle = tokio::spawn(async move {
        let channel = Endpoint::new(format!("http://127.0.0.1:{port}"))
            .unwrap()
            .connect_timeout(Duration::from_millis(100))
            .connect()
            .await
            .unwrap();
        let client = NodeClient::new(channel);
        let req = RequestBatchBuilder::new(0).transfer_leader(1, 1, 1).build();
        match client.batch_group_requests(req).await {
            Ok(_) => unreachable!(),
            Err(status) => {
                info!("message {} details {status:?}", status.message());
                assert!(!retryable_rpc_err(&status));
                assert!(transport_err(&status));
                let err = find_io_error(&status).unwrap();
                assert!(matches!(err.kind(), ErrorKind::ConnectionReset));
            }
        }
    });

    tokio::time::sleep(Duration::from_secs(1)).await;

    drop(socket);
    handle.await.unwrap();
}
