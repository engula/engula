// Copyright 2021 The Engula Authors.
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

use std::convert::Infallible;

use axum::{
    body::{Bytes, Full},
    http::{Response, StatusCode},
    response::IntoResponse,
    Json,
};
use serde_json::json;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("{0}")]
    InvalidArgument(String),
    #[error(transparent)]
    Unknown(#[from] Box<dyn std::error::Error + Send>),
}

impl IntoResponse for Error {
    type Body = Full<Bytes>;
    type BodyError = Infallible;

    fn into_response(self) -> Response<Self::Body> {
        let (code, message) = match self {
            Error::InvalidArgument(m) => (StatusCode::BAD_REQUEST, m),
            Error::Unknown(err) => (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()),
        };
        let resp = Json(json!({
            "error": {
                "code": code.as_u16(),
                "message": message,
            }
        }));
        (code, resp).into_response()
    }
}

pub type Result<T> = std::result::Result<T, Error>;
