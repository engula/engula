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
    collections::HashMap,
    sync::Arc,
    task::{Context, Poll},
};

use tonic::{
    body::BoxBody,
    codegen::{empty_body, http, BoxFuture, Service},
    transport::NamedService,
};

#[crate::async_trait]
pub(super) trait HttpHandle: Send + Sync {
    async fn call(
        &self,
        path: &str,
        params: &HashMap<String, String>,
    ) -> crate::Result<http::Response<String>>;
}

pub(super) struct Router {
    handles: HashMap<String, Box<dyn HttpHandle>>,
}

pub struct AdminService
where
    Self: Send + Sync,
{
    inner: Arc<Router>,
}

impl AdminService {
    pub(super) fn new(inner: Router) -> Self {
        AdminService {
            inner: Arc::new(inner),
        }
    }
}

impl<B> Service<http::Request<B>> for AdminService
where
    B: Send + 'static,
{
    type Response = http::Response<tonic::body::BoxBody>;
    type Error = std::convert::Infallible;
    type Future = BoxFuture<Self::Response, Self::Error>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: http::Request<B>) -> Self::Future {
        let inner = self.inner.clone();
        let query_params = req
            .uri()
            .query()
            .map(|q| {
                url::form_urlencoded::parse(q.as_bytes())
                    .into_owned()
                    .collect()
            })
            .unwrap_or_else(HashMap::new);
        let path = req.uri().path().to_owned();
        Box::pin(async move { inner.call(&path, query_params).await })
    }
}

impl NamedService for AdminService {
    const NAME: &'static str = "admin";
}

impl Clone for AdminService {
    fn clone(&self) -> Self {
        AdminService {
            inner: self.inner.clone(),
        }
    }
}

impl Router {
    pub fn empty() -> Self {
        Router {
            handles: HashMap::default(),
        }
    }

    pub fn nest(path: &str, r: Router) -> Self {
        if path.is_empty() || !path.starts_with('/') {
            panic!("Paths must start with a `/`");
        }

        let handles = r
            .handles
            .into_iter()
            .map(|(url, handle)| (format!("{path}{url}"), handle))
            .collect();
        Router { handles }
    }

    pub fn route(mut self, path: &str, handle: impl HttpHandle + 'static) -> Self {
        if path.is_empty() || !path.starts_with('/') {
            panic!("Paths must start with a `/`");
        }

        self.handles.insert(path.to_owned(), Box::new(handle));

        self
    }

    pub async fn call(
        &self,
        path: &str,
        params: HashMap<String, String>,
    ) -> Result<http::Response<BoxBody>, std::convert::Infallible> {
        let handle = match self.handles.get(path) {
            Some(handle) => handle,
            None => {
                return Ok(http::Response::builder()
                    .status(http::StatusCode::NOT_FOUND)
                    .body(empty_body())
                    .unwrap())
            }
        };

        let resp = match handle.call(path, &params).await {
            Ok(resp) => resp.map(boxed),
            Err(e) => http::Response::builder()
                .status(http::StatusCode::INTERNAL_SERVER_ERROR)
                .body(boxed(e.to_string()))
                .unwrap(),
        };

        Ok(resp)
    }
}

fn boxed(body: String) -> BoxBody {
    use http_body::Body;

    body.map_err(|_| panic!("")).boxed_unsync()
}
