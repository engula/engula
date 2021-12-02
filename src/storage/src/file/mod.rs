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
mod storage;

pub use self::storage::Storage;

#[cfg(test)]
mod tests {
    use std::{env, path::PathBuf, process};

    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use crate::*;

    struct TestEnvGuard {
        path: PathBuf,
    }

    impl TestEnvGuard {
        fn setup(case: &str) -> Self {
            let mut path = env::temp_dir();
            path.push("engula-storage-fs-test");
            path.push(process::id().to_string());
            path.push(case);
            Self { path }
        }
    }

    impl Drop for TestEnvGuard {
        fn drop(&mut self) {
            std::fs::remove_dir_all(&self.path).unwrap();
        }
    }

    #[tokio::test]
    async fn test_bucket_manage() -> Result<()> {
        const BUCKET_NAME: &str = "test_bucket";
        let g = TestEnvGuard::setup("test_bucket_manage");

        let s = super::Storage::new(&g.path).await?;
        s.create_bucket(BUCKET_NAME).await?;
        assert!(s.create_bucket(BUCKET_NAME).await.is_err());
        s.delete_bucket(BUCKET_NAME).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_object_manage() -> Result<()> {
        const BUCKET_NAME: &str = "test_object";
        let g = TestEnvGuard::setup("test_object_manage");

        let s = super::Storage::new(&g.path).await?;
        s.create_bucket(BUCKET_NAME).await?;

        let b = s.bucket(BUCKET_NAME).await?;

        let mut w = b.new_sequential_writer("obj-1").await?;
        w.write(b"abc").await?;
        w.write(b"123").await?;
        w.shutdown().await?;

        let mut r = b.new_sequential_reader("obj-1").await?;
        let mut got = Vec::new();
        r.read_to_end(&mut got).await?;
        assert_eq!(got, b"abc123");
        Ok(())
    }

    #[tokio::test]
    async fn test_duplicate_bucket() -> Result<()> {
        const BUCKET_NAME: &str = "test_bucket_dup";
        let g = TestEnvGuard::setup("test_bucket_duplicate");

        let s = super::Storage::new(&g.path).await?;
        s.create_bucket(BUCKET_NAME).await?;
        let r = s.create_bucket(BUCKET_NAME).await;
        assert!(r.is_err());
        assert!(matches!(r, Err(Error::AlreadyExists(_))));
        Ok(())
    }

    #[tokio::test]
    async fn test_clear_non_empty_bucket() -> Result<()> {
        const BUCKET_NAME: &str = "test_non_empty_delete";
        let g = TestEnvGuard::setup("test_non_empty_delete");
        let s = super::Storage::new(&g.path).await?;
        s.create_bucket(BUCKET_NAME).await?;
        let b = s.bucket(BUCKET_NAME).await?;
        let mut w = b.new_sequential_writer("obj-1").await?;
        w.write(b"abcd").await?;
        w.shutdown().await?;
        let r = s.delete_bucket(BUCKET_NAME).await;
        assert!(matches!(r, Err(Error::Io(_))));
        Ok(())
    }

    #[tokio::test]
    async fn test_put_duplicate_obj() -> Result<()> {
        const BUCKET_NAME: &str = "test_put_dup_obj";
        let g = TestEnvGuard::setup("test_put_dup_obj");
        let s = super::Storage::new(&g.path).await?;
        s.create_bucket(BUCKET_NAME).await?;
        let b = s.bucket(BUCKET_NAME).await?;

        let mut w = b.new_sequential_writer("obj-1").await?;
        w.write(b"abcdefg").await?;
        w.shutdown().await?;

        let mut w = b.new_sequential_writer("obj-1").await?;
        w.write(b"123").await?;
        w.shutdown().await?;

        let mut r = b.new_sequential_reader("obj-1").await?;
        let mut got = Vec::new();
        r.read_to_end(&mut got).await?;
        assert_eq!(got, b"123");

        Ok(())
    }

    #[tokio::test]
    async fn test_not_exist_bucket() -> Result<()> {
        const BUCKET_NAME: &str = "test_not_exist_bucket";
        let g = TestEnvGuard::setup("test_not_exist_bucket");
        let s = super::Storage::new(&g.path).await?;
        let b = s.bucket(BUCKET_NAME).await?;

        let r = b.delete_object("obj-1").await;
        assert!(matches!(r, Err(Error::NotFound(_))));

        let r = b.new_sequential_reader("obj").await;
        assert!(matches!(r, Err(Error::NotFound(_))));

        let w = b.new_sequential_writer("obj").await;
        assert!(matches!(w, Err(Error::NotFound(_))));

        Ok(())
    }
}
