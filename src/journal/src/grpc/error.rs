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

use crate::Error;

impl From<tonic::Status> for Error {
    fn from(s: tonic::Status) -> Self {
        Error::Unknown(s.to_string())
    }
}

impl From<tonic::transport::Error> for Error {
    fn from(e: tonic::transport::Error) -> Self {
        Error::Unknown(e.to_string())
    }
}

impl From<Error> for tonic::Status {
    fn from(err: Error) -> Self {
        let (code, message) = match err {
            Error::NotFound(s) => (tonic::Code::NotFound, s),
            Error::AlreadyExists(s) => (tonic::Code::AlreadyExists, s),
            Error::InvalidArgument(s) => (tonic::Code::InvalidArgument, s),
            Error::Unknown(s) => (tonic::Code::Unknown, s),
            Error::Io(e) => (tonic::Code::Unknown, e.to_string()),
        };
        tonic::Status::new(code, message)
    }
}
