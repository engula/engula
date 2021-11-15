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

use aws_sdk_s3::{error as s3_error, SdkError};
use thiserror::Error;

macro_rules! s3_to_engula_err {
    ( $( $x:ident ),* ) => {
            #[derive(Error, Debug)]
            pub enum Error {
            $(
                #[error(transparent)]
                $x(#[from] SdkError<s3_error::$x>),
            )*
            }
    };
}

s3_to_engula_err!(
    HeadBucketError,
    HeadObjectError,
    CreateBucketError,
    PutPublicAccessBlockError,
    ListObjectsError,
    ListBucketsError,
    DeleteBucketError,
    DeleteObjectError,
    DeleteObjectsError,
    CreateMultipartUploadError,
    UploadPartError,
    CompleteMultipartUploadError,
    GetObjectError
);

impl From<Error> for storage::Error {
    fn from(err: Error) -> Self {
        storage::Error::Unknown(err.into())
    }
}

pub fn to_storage_err<T: Into<Error>>(e: T) -> storage::Error {
    let e: Error = e.into();
    e.into()
}
