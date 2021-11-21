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

pub use std::borrow::Cow;

pub use self::storage::FileStorage;

#[cfg(test)]
mod tests {
    use std::{env, path::Path, process};

    use super::{error::Result, *};
    use crate::*;

    struct TestEnvGuard<'a> {
        path: Cow<'a, Path>,
    }

    impl<'a> TestEnvGuard<'a> {
        fn setup(case: &str) -> Self {
            let mut path = env::temp_dir();
            path.push("englua-storage-fs-test");
            path.push(process::id().to_string());
            path.push(case);
            Self { path: path.into() }
        }
    }

    impl<'a> Drop for TestEnvGuard<'a> {
        fn drop(&mut self) {
            std::fs::remove_dir_all(&self.path).unwrap();
        }
    }

    #[tokio::test]
    async fn test_bucket_manage() -> Result<()> {
        const BUCKET_NAME: &str = "test_bucket";
        let g = TestEnvGuard::setup("test_bucket_manage");

        let s = FileStorage::new(g.path.as_ref()).await?;
        s.create_bucket(BUCKET_NAME).await?;
        assert!(s.create_bucket(BUCKET_NAME).await.is_err());
        s.bucket(BUCKET_NAME).await?;
        s.delete_bucket(BUCKET_NAME).await?;
        assert!(s.bucket(BUCKET_NAME).await.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn test_object_manage() -> Result<()> {
        const BUCKET_NAME: &str = "test_object";
        let g = TestEnvGuard::setup("test_object_manage");

        let s = FileStorage::new(g.path.as_ref()).await?;
        let b = s.create_bucket(BUCKET_NAME).await?;

        let mut u = b.upload_object("obj-1").await?;
        u.write(b"abcd").await?;
        u.write(b"123").await?;
        u.finish().await?;

        let obj = b.object("obj-1").await?;
        let mut v = [0u8; 5];
        obj.read_at(&mut v[..], 2).await?;
        assert_eq!(&v[..], "cd123".as_bytes());
        Ok(())
    }

    #[tokio::test]
    async fn test_duplicate_bucket() -> Result<()> {
        const BUCKET_NAME: &str = "test_bucket_dup";
        let g = TestEnvGuard::setup("test_bucket_duplicate");

        let s = FileStorage::new(g.path.as_ref()).await?;
        s.create_bucket(BUCKET_NAME).await?;
        let r = s.create_bucket(BUCKET_NAME).await;
        assert!(r.is_err());
        assert_eq!(
            "`test_bucket_dup` already exists",
            r.err().unwrap().to_string()
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_clear_non_empty_bucket() -> Result<()> {
        const BUCKET_NAME: &str = "test_non_empty_delete";
        let g = TestEnvGuard::setup("test_non_empty_delete");
        let s = FileStorage::new(g.path.as_ref()).await?;
        let b = s.create_bucket(BUCKET_NAME).await?;
        let mut u = b.upload_object("obj-1").await?;
        u.write(b"abcd").await?;
        u.finish().await?;
        let r = s.delete_bucket(BUCKET_NAME).await;
        assert!(r.is_err());
        assert!(r.err().unwrap().to_string().contains("Directory not empty"));
        Ok(())
    }

    #[tokio::test]
    async fn test_put_duplicate_obj() -> Result<()> {
        const BUCKET_NAME: &str = "test_put_dup_obj";
        let g = TestEnvGuard::setup("test_put_dup_obj");
        let s = FileStorage::new(g.path.as_ref()).await?;
        let b = s.create_bucket(BUCKET_NAME).await?;

        let mut u = b.upload_object("obj-1").await?;
        u.write(b"abcdefg").await?;
        u.finish().await?;
        let mut u = b.upload_object("obj-1").await?;
        u.write(b"123").await?;
        u.finish().await?;

        let obj = b.object("obj-1").await?;
        let mut v = [0u8; 4];
        let n = obj.read_at(&mut v[..], 0).await?;
        assert_eq!(n, 3);
        assert_eq!(&v[..n], "123".as_bytes());

        let mut v = [0u8; 2];
        let n = obj.read_at(&mut v[..], 0).await?;
        assert_eq!(n, 2);
        assert_eq!(&v[..n], "12".as_bytes());
        Ok(())
    }
}
