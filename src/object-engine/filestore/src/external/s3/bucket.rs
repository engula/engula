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

use std::{os::unix::fs::FileExt, path::PathBuf};

use tokio::io::AsyncWriteExt;

use crate::{
    async_trait,
    store::blob_stream::{self, BlobStreamExt},
    RandomRead, Result, SequentialWrite,
};

const FILE_SUFFIX: &str = ".file";

#[derive(Clone)]
pub struct Bucket {
    path: PathBuf,
}

impl Bucket {
    pub(crate) fn new(path: PathBuf) -> Self {
        Self { path }
    }
}

#[async_trait]
impl crate::BlobStore for Bucket {
    async fn new_random_reader(&self, path: PathBuf) -> Result<Box<dyn RandomRead>> {
        let file = tokio::fs::File::open(&path).await.map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                crate::Error::NotFound(format!("file {:?}", path.file_name()))
            } else {
                e.into()
            }
        })?;
        Ok(Box::new(RandomReader {
            file: file.into_std().await,
        }))
    }

    async fn new_sequential_writer(&self, path: PathBuf) -> Result<Box<dyn SequentialWrite>> {
        let file = tokio::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .await?;
        Ok(Box::new(SequentialWriter { file }))
    }

    async fn list_files_by_prefix(
        &self,
        bucket_path: PathBuf,
        file_name_prefix: &str,
    ) -> Result<Vec<PathBuf>> {
        let res = tokio::fs::read_dir(bucket_path).await;
        if let Err(e) = &res {
            if e.kind() == std::io::ErrorKind::NotFound {
                return Ok(Vec::new());
            }
        }
        let mut read_dir = res?;
        let name_prefix = stream_file_prefix(file_name_prefix);
        let mut res = Vec::new();
        loop {
            let ent = read_dir.next_entry().await?;
            if ent.is_none() {
                break;
            }
            let path = ent.unwrap().path();
            if let Some(stem) = path.file_stem() {
                if Some(name_prefix.as_str()) != stem.to_str() {
                    continue;
                }
            }
            if path.extension().is_some() {
                res.push(path)
            }
        }
        Ok(res)
    }
}

fn stream_file_prefix(name: &str) -> String {
    format!("{}{}", name, blob_stream::STREAM_SUFFIX)
}

#[async_trait]
impl crate::Bucket for Bucket {
    async fn new_random_reader(&self, name: &str) -> Result<Box<dyn RandomRead>> {
        let path = self.path.join(name.to_owned() + FILE_SUFFIX);
        crate::BlobStore::new_random_reader(self, path).await
    }

    async fn new_sequential_writer(&self, name: &str) -> Result<Box<dyn SequentialWrite>> {
        let path = self.path.join(name.to_owned() + FILE_SUFFIX);
        crate::BlobStore::new_sequential_writer(self, path).await
    }

    async fn new_stream_reader(&self, file_name: &str) -> Result<Box<dyn crate::StreamReader>> {
        tokio::fs::create_dir_all(&self.path).await?;
        let bucket = self.to_owned();
        BlobStreamExt::new_stream_reader(bucket, self.path.to_owned(), file_name).await
    }

    async fn new_stream_writer(&self, file_name: &str) -> Result<Box<dyn crate::StreamWriter>> {
        tokio::fs::create_dir_all(&self.path).await?;
        let bucket = self.to_owned();
        BlobStreamExt::new_stream_writer(bucket, self.path.to_owned(), file_name).await
    }
}

pub struct RandomReader {
    file: std::fs::File,
}

#[async_trait]
impl crate::RandomRead for RandomReader {
    async fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> Result<()> {
        self.file.read_exact_at(buf, offset)?;
        Ok(())
    }
}

pub struct SequentialWriter {
    file: tokio::fs::File,
}

#[async_trait]
impl crate::SequentialWrite for SequentialWriter {
    async fn write(&mut self, buf: &[u8]) -> Result<()> {
        self.file.write_all(buf).await?;
        Ok(())
    }

    async fn finish(&mut self) -> Result<()> {
        self.file.sync_all().await?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use crate::{external::s3, *};

    #[tokio::test]
    async fn test_local_stream_rw() -> Result<()> {
        let tmp = tempdir::TempDir::new("test")?;
        let p = tmp.path();
        let s = s3::Store::open(p).await?;
        let t = s.create_tenant("t1").await?;
        let _ = t.create_bucket("b1").await?;
        let b = super::Bucket::new(p.join("b1").join("t1"));

        let mut w = b.new_stream_writer("s1").await?;
        w.append(StreamBlock::new(b"c1".to_vec())).await?;
        w.append(StreamBlock::new(b"c2".to_vec())).await?;
        w.append(StreamBlock::new(b"c3".to_vec())).await?;
        w.append(StreamBlock::new(b"c4".to_vec())).await?;
        w.flush().await?;

        let mut w1 = b.new_stream_writer("s1").await?;
        w.append(StreamBlock::new(b"z1".to_vec())).await?;
        w1.flush().await?;

        let mut r = b.new_stream_reader("s1").await?;
        let mut read_buf = StreamBlock::new(Vec::new());
        r.next(&mut read_buf).await?;
        assert_eq!(b"c1".to_vec(), read_buf.data());

        read_buf.reset();
        r.next(&mut read_buf).await?;
        assert_eq!(b"c2".to_vec(), read_buf.data());

        read_buf.reset();
        r.next(&mut read_buf).await?;
        assert_eq!(b"c3".to_vec(), read_buf.data());

        read_buf.reset();
        r.next(&mut read_buf).await?;
        assert_eq!(b"c4".to_vec(), read_buf.data());

        read_buf.reset();
        r.next(&mut read_buf).await?;
        assert_eq!(b"z1".to_vec(), read_buf.data());

        let mut r = b.new_stream_reader("s1").await?;
        let mut read_buf = StreamBlock::new(Vec::new());
        r.next(&mut read_buf).await?;
        assert_eq!(b"c1".to_vec(), read_buf.data());

        Ok(())
    }
}
