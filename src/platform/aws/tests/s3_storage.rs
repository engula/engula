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

use ::storage::*;
use aws::S3Storage;
use aws_sdk_s3::Config;
use aws_types::{region::Region, Credentials};
use http::Uri;
use smithy_http::endpoint::Endpoint;

const ACCESS_KEY: &str = "engulatest";
const SECRET_KEY: &str = "engulatest";
const TEST_REGION: &str = "us-east-2";

#[tokio::test]
#[ignore]
async fn integration_test_bucket_management() {
    let region = Some(Region::new(TEST_REGION));
    let credentials = Credentials::from_keys(ACCESS_KEY, SECRET_KEY, None);
    let endpoint = Endpoint::immutable(Uri::from_static("http://127.0.0.1:9000"));
    let config = Config::builder()
        .region(region)
        .credentials_provider(credentials)
        .endpoint_resolver(endpoint)
        .build();

    let storage = S3Storage::new(TEST_REGION, config);

    let bucket = "bucket-mng";
    let object = "object-0";

    storage.create_bucket(bucket).await.unwrap();
    let mut up = storage.upload_object(bucket, object).await.unwrap();
    let buf = vec![0, 1, 2];
    up.write(&buf).await.unwrap();
    let len = up.finish().await.unwrap();
    assert_eq!(len, buf.len());
    let o = storage.object(bucket, object).await.unwrap();
    let mut got = vec![0; buf.len()];
    o.read_at(&mut got, 0).await.unwrap();
    assert_eq!(got, buf);

    storage.delete_object(bucket, object).await.unwrap();
    storage.delete_bucket(bucket).await.unwrap();
}

#[tokio::test]
#[ignore]
async fn integration_test_multipart_put() {
    let region = Some(Region::new(TEST_REGION));
    let credentials = Credentials::from_keys(ACCESS_KEY, SECRET_KEY, None);
    let endpoint = Endpoint::immutable(Uri::from_static("http://127.0.0.1:9000"));
    let config = Config::builder()
        .region(region)
        .credentials_provider(credentials)
        .endpoint_resolver(endpoint)
        .build();

    let storage = S3Storage::new(TEST_REGION, config);

    let bucket = "multipart-put";
    let object = "object-0";

    storage.create_bucket(bucket).await.unwrap();
    let mut up = storage.upload_object(bucket, object).await.unwrap();
    up.write("123".as_bytes()).await.unwrap();
    up.finish().await.unwrap();

    let reader = storage.object(bucket, object).await.unwrap();
    let mut buf: [u8; 2] = [0; 2];
    let rs = reader.read_at(&mut buf[..], 0).await.unwrap();
    assert_eq!(rs, 2);
    assert_eq!(&buf[..], b"12");

    storage.delete_object(bucket, object).await.unwrap();
    storage.delete_bucket(bucket).await.unwrap();
}
