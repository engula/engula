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
    pin::Pin,
    task::{Context, Poll},
};

use futures::Stream;
use hyper::server::{
    accept::Accept,
    conn::{AddrIncoming, AddrStream},
};
use tokio::net::TcpListener;

#[derive(Debug)]
pub struct TcpIncoming {
    inner: AddrIncoming,
}

impl TcpIncoming {
    pub fn from_listener(listener: TcpListener, nodelay: bool) -> Self {
        let mut incoming =
            AddrIncoming::from_listener(listener).expect("TcpIncoming::from_listener");
        incoming.set_nodelay(nodelay);
        TcpIncoming { inner: incoming }
    }
}

impl Stream for TcpIncoming {
    type Item = Result<AddrStream, std::io::Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.inner).poll_accept(cx)
    }
}
