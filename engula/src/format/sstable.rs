use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use tokio::io::AsyncWriteExt;

use super::block::{BlockBuilder, BlockHandle, BlockIterator, BLOCK_HANDLE_SIZE};
use super::iterator::Iterator;
use super::table::{TableBuilder, TableReader};
use super::two_level_iterator::{BlockIterGenerator, TwoLevelIterator};
use super::Timestamp;
use crate::error::{Error, Result};
use crate::file_system::{RandomAccessReader, SequentialWriter};

#[derive(Clone, Debug)]
pub struct SstOptions {
    pub block_size: usize,
}

impl SstOptions {
    pub fn default() -> SstOptions {
        SstOptions { block_size: 8192 }
    }
}

pub const FOOTER_SIZE: usize = BLOCK_HANDLE_SIZE;

pub struct SstFooter {
    index_handle: BlockHandle,
}

impl SstFooter {
    fn decode_from(buf: &[u8]) -> SstFooter {
        SstFooter {
            index_handle: BlockHandle::decode_from(buf),
        }
    }

    fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&self.index_handle.encode());
        buf
    }
}

pub struct SstBuilder {
    options: SstOptions,
    file: SstFileWriter,
    error: Option<Error>,
    last_ts: Timestamp,
    last_key: Vec<u8>,
    data_block: BlockBuilder,
    index_block: BlockBuilder,
}

impl SstBuilder {
    pub fn new(options: SstOptions, file: Box<dyn SequentialWriter>) -> SstBuilder {
        SstBuilder {
            options,
            file: SstFileWriter::new(file),
            error: None,
            last_ts: 0,
            last_key: Vec::new(),
            data_block: BlockBuilder::new(),
            index_block: BlockBuilder::new(),
        }
    }

    async fn flush_data_block(&mut self) -> Result<()> {
        let block = self.data_block.finish();
        let block_handle = self.file.write_block(block).await?;
        let encoded_handle = block_handle.encode();
        self.index_block
            .add(self.last_ts, &self.last_key, &encoded_handle);
        self.data_block.reset();
        Ok(())
    }
}

#[async_trait]
impl TableBuilder for SstBuilder {
    async fn add(&mut self, ts: Timestamp, key: &[u8], value: &[u8]) {
        if self.error.is_some() {
            return;
        }
        let this_key = key.to_owned();
        assert!(this_key > self.last_key || (this_key == self.last_key && ts < self.last_ts));
        self.last_ts = ts;
        self.last_key = this_key;
        self.data_block.add(ts, key, value);
        if self.data_block.approximate_size() >= self.options.block_size {
            if let Err(error) = self.flush_data_block().await {
                self.error = Some(error);
            }
        }
    }

    async fn finish(&mut self) -> Result<usize> {
        if let Some(error) = &self.error {
            return Err(error.clone());
        }
        if self.data_block.approximate_size() > 0 {
            self.flush_data_block().await?;
        }
        if self.index_block.approximate_size() > 0 {
            let block = self.index_block.finish();
            let index_handle = self.file.write_block(block).await?;
            let footer = SstFooter { index_handle };
            let encoded_footer = footer.encode();
            let _ = self.file.write_block(&encoded_footer).await?;
        }
        self.file.sync_data().await?;
        Ok(self.file.file_size())
    }
}

struct SstFileWriter {
    file: Pin<Box<dyn SequentialWriter>>,
    offset: usize,
}

impl SstFileWriter {
    fn new(file: Box<dyn SequentialWriter>) -> SstFileWriter {
        SstFileWriter {
            file: Pin::new(file),
            offset: 0,
        }
    }

    fn file_size(&self) -> usize {
        self.offset
    }

    async fn sync_data(&self) -> Result<()> {
        self.file.sync_data().await
    }

    async fn write_block(&mut self, block: &[u8]) -> Result<BlockHandle> {
        let handle = BlockHandle {
            offset: self.offset as u64,
            size: block.len() as u64,
        };
        self.file.write_all(block).await?;
        self.offset += block.len();
        Ok(handle)
    }
}

pub struct SstReader {
    file: Arc<dyn RandomAccessReader>,
    index_block: Arc<Vec<u8>>,
}

impl SstReader {
    pub async fn open(file: Box<dyn RandomAccessReader>, size: u64) -> Result<SstReader> {
        assert!(size >= FOOTER_SIZE as u64);
        let mut footer_data = [0; FOOTER_SIZE];
        file.read_at(&mut footer_data, size - FOOTER_SIZE as u64)
            .await?;
        let footer = SstFooter::decode_from(&footer_data);
        let mut index_block = Vec::new();
        index_block.resize(footer.index_handle.size as usize, 0);
        file.read_at(&mut index_block, footer.index_handle.offset)
            .await?;
        Ok(SstReader {
            file: Arc::from(file),
            index_block: Arc::new(index_block),
        })
    }
}

#[async_trait]
impl TableReader for SstReader {
    async fn get(&self, ts: Timestamp, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let mut iter = self.new_iterator().await?;
        iter.seek(ts, key).await;
        if let Some(error) = iter.error() {
            return Err(error);
        }
        if let Some(version) = iter.current() {
            if version.0 == ts && version.1 == key {
                return Ok(Some(version.2.to_owned()));
            }
        }
        Ok(None)
    }

    async fn new_iterator(&self) -> Result<Box<dyn Iterator>> {
        let index_iter = BlockIterator::new(self.index_block.clone());
        let block_iter_generator = SstBlockIterGenerator::new(self.file.clone());
        let two_level_iter =
            TwoLevelIterator::new(Box::new(index_iter), Box::new(block_iter_generator));
        Ok(Box::new(two_level_iter))
    }
}

pub struct SstBlockIterGenerator {
    file: Arc<dyn RandomAccessReader>,
}

impl SstBlockIterGenerator {
    fn new(file: Arc<dyn RandomAccessReader>) -> SstBlockIterGenerator {
        SstBlockIterGenerator { file }
    }
}

#[async_trait]
impl BlockIterGenerator for SstBlockIterGenerator {
    async fn spawn(&self, index_value: &[u8]) -> Result<Box<dyn Iterator>> {
        let block_handle = BlockHandle::decode_from(index_value);
        let mut block = Vec::new();
        block.resize(block_handle.size as usize, 0);
        let block_size = self.file.read_at(&mut block, block_handle.offset).await?;
        assert_eq!(block_size as u64, block_handle.size);
        Ok(Box::new(BlockIterator::new(Arc::new(block))))
    }
}

#[cfg(test)]
mod tests {
    use super::super::*;
    use super::*;
    use crate::file_system::{FileSystem, LocalFileSystem};

    #[tokio::test]
    async fn test() {
        let fs = LocalFileSystem::new("/tmp/engula_test").unwrap();
        let wfile = fs.new_sequential_writer("test.sst").await.unwrap();
        let options = SstOptions::default();
        let mut builder = SstBuilder::new(options, wfile);
        let num_versions = 1024u64;
        for i in 0..num_versions {
            let v = i.to_be_bytes();
            builder.add(i, &v, &v).await;
        }
        let file_size = builder.finish().await.unwrap();
        let file_reader = fs.new_random_access_reader("test.sst").await.unwrap();
        let reader = SstReader::open(file_reader, file_size as u64)
            .await
            .unwrap();
        let mut iter = reader.new_iterator().await.unwrap();
        assert!(!iter.valid());
        iter.seek_to_first().await;
        for i in 0..num_versions {
            let v = i.to_be_bytes();
            assert_eq!(iter.current(), Some(Version(i, &v, &v)));
            iter.next().await;
        }
        assert_eq!(iter.current(), None);
        let ts = num_versions / 2;
        let target = ts.to_be_bytes();
        iter.seek(ts, &target).await;
        assert_eq!(iter.current(), Some(Version(ts, &target, &target)));
        for i in 0..num_versions {
            let expect = i.to_be_bytes();
            let actual = reader.get(i, &expect).await.unwrap().unwrap();
            assert_eq!(&actual, &expect);
        }
    }
}
