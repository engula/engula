// Copyright 2022 Engula Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use engula_master::{Client, Master, MemberId, NodeDescriptor, Server};
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;

#[tokio::test]
async fn master_liveness() -> anyhow::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;

    let master = Master::default();
    let server = Server::new(master);
    tokio::task::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(server.into_service())
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap()
    });

    let client = Client::connect(addr.to_string()).await?;
    let id = MemberId { id: 256 };
    let desc = NodeDescriptor {
        id: None,
        addr: Some("127.0.0.1:5678".to_string()),
    };

    let ids = client.join_member(vec![desc.clone()]).await?;
    assert_eq!(ids.len(), 1);
    assert_eq!(ids[0], id.clone());

    let descs = client.lookup_member(vec![id.clone()]).await?;
    assert_eq!(descs.len(), 1);
    assert_eq!(descs[0], desc.clone());

    let ids = client.leave_member(vec![id.clone()]).await?;
    assert_eq!(ids.len(), 1);
    assert_eq!(ids[0], id.clone());

    let ids = client.leave_member(vec![id.clone()]).await?;
    assert_eq!(ids.len(), 0);

    let descs = client.lookup_member(vec![id.clone()]).await?;
    assert_eq!(descs.len(), 0);

    Ok(())
}
