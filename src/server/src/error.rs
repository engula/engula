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

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("invalid {0}")]
    Invalid(String),

    #[error("staled request to {0}")]
    StaledRequest(u64),

    #[error("invalid lease")]
    InvalidLease,

    #[error("transport {0}")]
    Transport(#[from] tonic::transport::Error),

    #[error("io {0}")]
    Io(#[from] std::io::Error),

    #[error("rocksdb {0}")]
    RocksDb(#[from] rocksdb::Error),
}

pub type Result<T> = std::result::Result<T, Error>;

impl From<Error> for tonic::Status {
    fn from(e: Error) -> Self {
        use tonic::Status;

        // FIXME(walter) error details.
        match e {
            Error::Invalid(msg) => Status::invalid_argument(msg),
            Error::InvalidLease => Status::failed_precondition(""),
            Error::StaledRequest(group_id) => Status::failed_precondition(group_id.to_string()),
            Error::Transport(inner) => Status::unknown(inner.to_string()),
            Error::Io(inner) => Status::unknown(inner.to_string()),
            Error::RocksDb(inner) => Status::unknown(inner.to_string()),
        }
    }
}

impl From<futures::channel::oneshot::Canceled> for Error {
    fn from(_: futures::channel::oneshot::Canceled) -> Self {
        todo!()
    }
}
