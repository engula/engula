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

mod bucket;
mod error;
mod object;
mod storage;

pub use self::storage::S3Storage;

#[cfg(test)]
mod tests {
    use ::storage::*;

    use super::*;

    const ACCESS_KEY: &str = "";
    const SECRET_KEY: &str = "";

    #[tokio::test]
    #[ignore]
    async fn test_bucket_management() {
        const TEST_REGION: &str = "us-east-2";

        let storage = S3Storage::new(TEST_REGION, ACCESS_KEY, SECRET_KEY);

        let bucket = "testd-bucket-mng";

        storage.create_bucket(bucket).await.unwrap();

        storage.delete_bucket(bucket).await.unwrap();
    }

    #[tokio::test]
    #[ignore]
    async fn test_multipart_put() {
        const TEST_REGION: &str = "us-east-2";
        let bucket_name = "testd-multipart-put-test";
        let key = "test-obj";

        let storage = S3Storage::new(TEST_REGION, ACCESS_KEY, SECRET_KEY);

        storage
            .create_bucket(&bucket_name.to_owned())
            .await
            .unwrap();

        let bucket = storage.bucket(&bucket_name.to_owned()).await.unwrap();

        let mut writer = bucket.upload_object(key).await.unwrap();

        writer.write("123".as_bytes()).await.unwrap();

        writer.finish().await.unwrap();

        let reader = bucket.object(key).await.unwrap();

        let mut buf: [u8; 2] = [0; 2];
        let rs = reader.read_at(&mut buf[..], 0).await.unwrap();
        assert_eq!(rs, 2);
        assert_eq!(&buf[..], b"12");

        storage
            .delete_bucket(&bucket_name.to_owned())
            .await
            .unwrap();
    }
}
