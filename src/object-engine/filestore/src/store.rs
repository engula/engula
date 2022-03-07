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

use tokio::io::{AsyncRead, AsyncReadExt};

use self::blob_stream::{BlobStore, BlobStreamExt};
use crate::{async_trait, Result};

#[async_trait]
pub trait Store: Send + Sync {
    fn tenant(&self, name: &str) -> Box<dyn Tenant>;

    async fn create_tenant(&self, name: &str) -> Result<Box<dyn Tenant>>;
}

#[async_trait]
pub trait Tenant: Send + Sync {
    fn bucket(&self, name: &str) -> Box<dyn Bucket>;

    async fn create_bucket(&self, name: &str) -> Result<Box<dyn Bucket>>;
}

#[async_trait]
pub trait Bucket: Send + Sync {
    async fn new_random_reader(&self, name: &str) -> Result<Box<dyn RandomRead>>;

    async fn new_sequential_writer(&self, name: &str) -> Result<Box<dyn SequentialWrite>>;

    async fn new_stream_reader(&self, file_name: &str) -> Result<Box<dyn StreamReader>>;

    async fn new_stream_writer(&self, file_name: &str) -> Result<Box<dyn StreamWriter>>;
}

#[async_trait]
pub trait RandomRead: Send + Sync + 'static {
    async fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> Result<()>;
}

#[async_trait]
pub trait SequentialWrite: Send + Sync + 'static {
    async fn write(&mut self, buf: &[u8]) -> Result<()>;

    async fn finish(&mut self) -> Result<()>;
}

#[async_trait]
pub trait StreamReader {
    async fn next(&mut self, buf: &mut StreamBlock) -> Result<()>;
}

#[async_trait]
pub trait StreamWriter {
    async fn append(&mut self, buf: StreamBlock) -> Result<()>;
    async fn flush(&mut self) -> Result<()>;
    async fn state(&self) -> Result<StreamState>;
}

pub enum StreamState {
    SingleFile { data_size: i64 },
    MultiFile { file_count: u32 },
}

pub struct StreamBlock {
    data: Vec<u8>,
}

impl StreamBlock {
    pub fn new(data: Vec<u8>) -> Self {
        Self { data }
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(self.data.len() + 8);
        buf.extend_from_slice(&(self.data.len() as u64).to_be_bytes());
        buf.extend_from_slice(&self.data);
        buf
    }

    pub async fn decode_from_async_read<R>(&mut self, r: &mut R) -> Result<()>
    where
        R: AsyncRead + Unpin,
    {
        let len = r.read_u64().await?;
        let mut buf = vec![0u8; len as usize];
        r.read_exact(&mut buf).await?;
        self.data.extend(&buf);
        Ok(())
    }

    pub async fn decode_from_random_read(&mut self, r: Box<dyn RandomRead>) -> Result<()> {
        let mut len_buf = [0u8; 8];
        r.read_exact_at(&mut len_buf, 0).await?;
        let len = u64::from_be_bytes(len_buf);
        let mut data_buf = vec![0u8; len as usize];
        r.read_exact_at(data_buf.as_mut(), 8).await?;
        self.data.extend(&data_buf);
        Ok(())
    }

    pub fn data(&self) -> Vec<u8> {
        self.data.to_owned()
    }

    pub fn reset(&mut self) {
        self.data.clear()
    }
}

pub mod blob_stream {

    use std::path::{Path, PathBuf};

    use super::*;
    use crate::{async_trait, Error, Result};

    pub const STREAM_SUFFIX: &str = ".stream";
    const CONFLICT_FILE_NAME_MAX_RETRY: u32 = 20;

    const STREAM_FILE_INIT_SEQ: u64 = 1;

    #[async_trait]
    pub trait BlobStore: Send + Sync {
        async fn new_random_reader(&self, path: PathBuf) -> Result<Box<dyn RandomRead>>;

        async fn new_sequential_writer(&self, path: PathBuf) -> Result<Box<dyn SequentialWrite>>;

        async fn list_files_by_prefix(
            &self,
            bucket_path: PathBuf,
            file_name_prefix: &str,
        ) -> Result<Vec<PathBuf>>;
    }

    #[async_trait]
    impl<T: ?Sized + BlobStore> BlobStore for &T {
        async fn new_random_reader(&self, path: PathBuf) -> Result<Box<dyn RandomRead>> {
            self.new_random_reader(path).await
        }

        async fn new_sequential_writer(&self, path: PathBuf) -> Result<Box<dyn SequentialWrite>> {
            self.new_sequential_writer(path).await
        }

        async fn list_files_by_prefix(
            &self,
            bucket_path: PathBuf,
            file_name_prefix: &str,
        ) -> Result<Vec<PathBuf>> {
            self.list_files_by_prefix(bucket_path, file_name_prefix)
                .await
        }
    }

    #[async_trait]
    pub trait BlobStreamExt: BlobStore {
        async fn new_stream_reader(
            self,
            bucket_path: PathBuf,
            stream_name: &str,
        ) -> Result<Box<dyn crate::StreamReader>>
        where
            Self: Sized + 'static,
        {
            let read_file_seq = STREAM_FILE_INIT_SEQ;
            let last_file_seq = self
                .current_last_file_seq(bucket_path.to_owned(), stream_name)
                .await?;
            Ok(Box::new(StreamReader {
                blobstore: self,
                bucket_path,
                stream_name: stream_name.to_owned(),
                read_file_seq,
                last_file_seq,
            }))
        }

        async fn new_stream_writer(
            self,
            bucket_path: PathBuf,
            stream_name: &str,
        ) -> Result<Box<dyn crate::StreamWriter>>
        where
            Self: Sized + 'static,
        {
            let last_file_seq = self
                .current_last_file_seq(bucket_path.to_owned(), stream_name)
                .await?;
            Ok(Box::new(StreamWriter {
                blob: self,
                bucket_path,
                stream_name: stream_name.to_owned(),
                last_file_seq,
            }))
        }

        async fn current_last_file_seq(
            &self,
            bucket_path: PathBuf,
            file_prefix: &str,
        ) -> Result<u64> {
            let mut last_file_seq = STREAM_FILE_INIT_SEQ - 1;
            let paths = self
                .list_files_by_prefix(bucket_path.to_owned(), file_prefix)
                .await?;
            for path in paths {
                if let Some(ext) = path.extension() {
                    if let Ok(seq) = ext.to_str().unwrap().parse::<u64>() {
                        if seq > last_file_seq {
                            last_file_seq = seq;
                        }
                    }
                }
            }
            Ok(last_file_seq)
        }
    }

    pub struct StreamReader<B>
    where
        B: BlobStore,
    {
        blobstore: B,
        bucket_path: PathBuf,
        stream_name: String,
        last_file_seq: u64,
        read_file_seq: u64,
    }

    #[async_trait]
    impl<B> crate::StreamReader for StreamReader<B>
    where
        B: BlobStore,
    {
        async fn next(&mut self, block: &mut crate::StreamBlock) -> Result<()> {
            if self.read_file_seq > self.last_file_seq {
                return Ok(());
            }
            let stream_record =
                stream_file_path(&self.bucket_path, &self.stream_name, self.read_file_seq);
            let reader = self
                .blobstore
                .new_random_reader(stream_record.to_owned())
                .await?;
            block.decode_from_random_read(reader).await?;
            self.read_file_seq += 1;
            Ok(())
        }
    }

    pub struct StreamWriter<B>
    where
        B: BlobStore,
    {
        blob: B,
        bucket_path: PathBuf,
        stream_name: String,
        last_file_seq: u64,
    }

    #[async_trait]
    impl<B> crate::StreamWriter for StreamWriter<B>
    where
        B: BlobStore,
    {
        async fn append(&mut self, block: crate::StreamBlock) -> Result<()> {
            let attempt_seq = self.last_file_seq;
            let new_seq = self
                .write_seq_file_with_retry_iteratively(block, attempt_seq)
                .await?;
            self.last_file_seq = new_seq;
            Ok(())
        }

        async fn flush(&mut self) -> Result<()> {
            Ok(())
        }

        async fn state(&self) -> Result<StreamState> {
            let file_count = self.last_file_seq as u32;
            Ok(StreamState::MultiFile { file_count })
        }
    }

    impl<B> StreamWriter<B>
    where
        B: BlobStore,
    {
        async fn write_seq_file_with_retry_iteratively(
            &self,
            block: crate::StreamBlock,
            last_seq: u64,
        ) -> Result<u64> {
            let mut attempt_seq = last_seq + 1;
            let mut attempt_count = 0;
            loop {
                let stream_file =
                    stream_file_path(&self.bucket_path, &self.stream_name, attempt_seq);
                let result = (|| async {
                    let res = self
                        .blob
                        .new_sequential_writer(stream_file.to_owned())
                        .await;
                    if let Err(e) = res {
                        return Err(e);
                    }
                    let buf = block.encode();
                    res.unwrap().write(&buf).await?;
                    Ok(())
                })()
                .await;
                if let Err(Error::AlreadyExists(_)) = result {
                    attempt_count += 1;
                    if attempt_count < CONFLICT_FILE_NAME_MAX_RETRY {
                        attempt_seq += 1;
                        continue;
                    }
                }
                result?;
                return Ok(attempt_seq);
            }
        }
    }

    fn stream_file_path(bucket_path: impl AsRef<Path>, name: &str, seq: u64) -> PathBuf {
        bucket_path
            .as_ref()
            .join(format!("{}{}.{:0>20}", name, STREAM_SUFFIX, seq))
    }
}

impl<B: BlobStore + 'static> BlobStreamExt for B {}
