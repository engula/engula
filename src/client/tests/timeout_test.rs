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

use std::{net::SocketAddr, time::Duration};

use engula_client::{error::retryable_rpc_err, NodeClient, RequestBatchBuilder, RpcTimeout};
use socket2::{Domain, Socket, Type};
use tonic::{transport::Endpoint, Code};

#[ctor::ctor]
fn init() {
    tracing_subscriber::fmt::init();
}

#[tokio::test]
async fn connect_timeout_report_timed_out() {
    // Connect to a non-routable IP address.
    let channel = Endpoint::new("http://10.255.255.1:1234".to_owned())
        .unwrap()
        .connect_timeout(Duration::from_millis(100))
        .connect_lazy();
    let client = NodeClient::new(channel);
    let req = RequestBatchBuilder::new(0).transfer_leader(1, 1, 1).build();
    match client
        .batch_group_requests(RpcTimeout::new(Some(Duration::from_secs(3)), req))
        .await
    {
        Ok(_) => unreachable!(),
        Err(status) => {
            assert!(
                retryable_rpc_err(&status),
                "Expect Code::TimedOut, but got {status:?}"
            );
        }
    }
}

#[tokio::test]
async fn rpc_timeout_report_canceled() {
    let socket = Socket::new(Domain::IPV4, Type::STREAM, None).unwrap();
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
        .connect_lazy();
    let client = NodeClient::new(channel);
    let req = RequestBatchBuilder::new(0).transfer_leader(1, 1, 1).build();
    match client
        .batch_group_requests(RpcTimeout::new(Some(Duration::from_millis(100)), req))
        .await
    {
        Ok(_) => unreachable!(),
        Err(status) => {
            assert!(
                matches!(status.code(), Code::Cancelled),
                "Expect Code::Cancelled, got {status:?}"
            );
        }
    }
}
