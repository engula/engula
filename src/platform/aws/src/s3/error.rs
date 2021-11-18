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

#[derive(Error, Debug)]
pub enum Error {
    #[error(transparent)]
    HeadBucket(#[from] SdkError<s3_error::HeadBucketError>),
    #[error(transparent)]
    HeadObject(#[from] SdkError<s3_error::HeadObjectError>),
    #[error(transparent)]
    CreateBucket(#[from] SdkError<s3_error::CreateBucketError>),
    #[error(transparent)]
    PutPublicAccessBlock(#[from] SdkError<s3_error::PutPublicAccessBlockError>),
    #[error(transparent)]
    ListObjects(#[from] SdkError<s3_error::ListObjectsError>),
    #[error(transparent)]
    ListObjectsV2(#[from] SdkError<s3_error::ListObjectsV2Error>),
    #[error(transparent)]
    DeleteBucket(#[from] SdkError<s3_error::DeleteBucketError>),
    #[error(transparent)]
    DeleteObject(#[from] SdkError<s3_error::DeleteObjectError>),
    #[error(transparent)]
    DeleteObjects(#[from] SdkError<s3_error::DeleteObjectsError>),
    #[error(transparent)]
    CreateMultipartUpload(#[from] SdkError<s3_error::CreateMultipartUploadError>),
    #[error(transparent)]
    UploadPart(#[from] SdkError<s3_error::UploadPartError>),
    #[error(transparent)]
    CompleteMultipartUpload(#[from] SdkError<s3_error::CompleteMultipartUploadError>),
    #[error(transparent)]
    GetObject(#[from] SdkError<s3_error::GetObjectError>),
    #[error(transparent)]
    WaitUploadTaskDone(#[from] tokio::task::JoinError),
    #[error(transparent)]
    ReadObjectBody(#[from] smithy_http::byte_stream::Error),
}

pub type Result<T> = std::result::Result<T, Error>;
