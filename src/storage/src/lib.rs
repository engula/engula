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

mod bucket_handle;
mod error;
mod object_handle;
mod object_storage;

#[cfg(feature = "aws-s3")]
mod aws_s3;

pub use self::{
    bucket_handle::BucketHandle,
    error::{StorageError, StorageResult},
    object_handle::{ObjectReader, ObjectWriter},
    object_storage::ObjectStorage,
};

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::{ObjectStorage, aws_s3::RemoteS3Storage};

    const ACCESS_KEY: &str = "<your key>";
    const SECRET_KEY: &str = "<your secret>";

    #[tokio::test]
    #[ignore]
    async fn test_bucket_management() {
        const TEST_REGION: &str = "us-east-2";

        let storage = RemoteS3Storage::new(TEST_REGION, ACCESS_KEY, SECRET_KEY);

        let buckets = storage.list_buckets().await.unwrap();
        assert_eq!(buckets.len(), 0);

        let bucket = "testd-bucket-mng";

        storage.create_bucket(bucket, TEST_REGION).await.unwrap();

        let buckets = storage.list_buckets().await.unwrap();
        assert_eq!(buckets.len(), 1);

        storage.delete_bucket(bucket).await.unwrap();

        let buckets = storage.list_buckets().await.unwrap();
        assert_eq!(buckets.len(), 0);
    }

    #[tokio::test]
    #[ignore]
    async fn test_simple_put() {
        const TEST_REGION: &str = "us-east-2";
        let bucket = "testd-simple-put";

        let storage = RemoteS3Storage::new(TEST_REGION, ACCESS_KEY, SECRET_KEY);

        storage.create_bucket(bucket, TEST_REGION).await.unwrap();

        let bucket_handle = storage.bucket(bucket);

        bucket_handle
            .put_object("test-key", Bytes::from("hello test"))
            .await
            .unwrap();
    }

    #[tokio::test]
    #[ignore]
    async fn test_multipart_put() {
        const TEST_REGION: &str = "us-east-2";
        let bucket = "testd-multipart-put";
        let key = "test-obj";

        let storage = RemoteS3Storage::new(TEST_REGION, ACCESS_KEY, SECRET_KEY);

        storage.create_bucket(bucket, TEST_REGION).await.unwrap();

        let bucket_handle = storage.bucket(bucket);

        let mut writer = bucket_handle.new_writer(key).await.unwrap();

        writer.write(Bytes::from("123")).await;

        writer.finish().await.unwrap();

        let reader = bucket_handle.new_reader(key).await;

        let rs = reader.read_at(0, 2).await.unwrap();

        assert_eq!(rs, b"12");
    }
}
