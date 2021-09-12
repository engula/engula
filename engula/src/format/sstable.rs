use std::sync::Arc;

use async_trait::async_trait;
use bytes::BufMut;

use super::{
    block::{BlockBuilder, BlockHandle, BlockIterator, BLOCK_HANDLE_SIZE},
    iterator::Iterator,
    table::{TableBuilder, TableReader},
    two_level_iterator::{BlockIterGenerator, TwoLevelIterator},
    TableDesc, Timestamp,
};
use crate::{
    cache::Cache,
    error::{Error, Result},
    fs::{RandomAccessReader, SequentialWriter},
};

#[derive(Clone, Debug)]
pub struct SstOptions {
    pub block_size: usize,
}

impl SstOptions {
    pub fn default() -> SstOptions {
        SstOptions { block_size: 4096 }
    }
}

pub struct SstBuilder {
    options: SstOptions,
    file: SstFileWriter,
    done: bool,
    error: Option<Error>,
    last_ts: Timestamp,
    last_key: Vec<u8>,
    data_block: BlockBuilder,
    index_block: BlockBuilder,
}

impl SstBuilder {
    pub fn new(
        options: SstOptions,
        table_writer: Box<dyn SequentialWriter>,
        table_number: u64,
    ) -> SstBuilder {
        SstBuilder {
            options,
            file: SstFileWriter::new(table_writer, table_number),
            done: false,
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
        self.index_block
            .add(self.last_ts, &self.last_key, &block_handle.encode());
        self.data_block.reset();
        Ok(())
    }
}

#[async_trait]
impl TableBuilder for SstBuilder {
    async fn add(&mut self, ts: Timestamp, key: &[u8], value: &[u8]) {
        assert!(!self.done);
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

    async fn finish(&mut self) -> Result<TableDesc> {
        assert!(!self.done);
        self.done = true;
        if let Some(error) = &self.error {
            return Err(error.clone());
        }
        if self.data_block.approximate_size() > 0 {
            self.flush_data_block().await?;
        }
        let index_block = self.index_block.finish();
        let index_handle = self.file.write_block(index_block).await?;
        let footer = SstFooter { index_handle };
        let _ = self.file.write_block(&footer.encode()).await?;
        self.file.finish().await
    }
}

pub const FOOTER_SIZE: u64 = BLOCK_HANDLE_SIZE;

struct SstFooter {
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

struct SstFileWriter {
    file: Box<dyn SequentialWriter>,
    number: u64,
    offset: u64,
}

impl SstFileWriter {
    fn new(file: Box<dyn SequentialWriter>, number: u64) -> SstFileWriter {
        SstFileWriter {
            file,
            number,
            offset: 0,
        }
    }

    async fn write_block(&mut self, block: &[u8]) -> Result<BlockHandle> {
        let handle = BlockHandle {
            offset: self.offset,
            size: block.len() as u64,
        };
        self.file.write(block).await?;
        self.offset += block.len() as u64;
        Ok(handle)
    }

    async fn finish(&mut self) -> Result<TableDesc> {
        self.file.finish().await?;
        Ok(TableDesc {
            table_number: self.number,
            table_size: self.offset,
        })
    }
}

const CACHE_KEY_LEN: usize = 16;

fn make_cache_key(mut buf: &mut [u8], number: u64, offset: u64) {
    buf.put_u64_le(number);
    buf.put_u64_le(offset);
}

struct SstCache {
    cache: Option<Arc<dyn Cache>>,
}

impl SstCache {
    fn new(cache: Option<Arc<dyn Cache>>) -> SstCache {
        SstCache { cache }
    }

    async fn get(&self, number: u64, offset: u64) -> Option<Arc<Vec<u8>>> {
        if let Some(cache) = self.cache.as_ref() {
            let mut key = [0; CACHE_KEY_LEN];
            make_cache_key(&mut key, number, offset);
            cache.get(&key).await
        } else {
            None
        }
    }

    async fn put(&self, number: u64, offset: u64, value: Arc<Vec<u8>>) {
        if let Some(cache) = self.cache.as_ref() {
            let mut key = Vec::new();
            make_cache_key(&mut key, number, offset);
            cache.put(key, value).await;
        }
    }
}

struct SstFileReader {
    desc: TableDesc,
    file: Box<dyn RandomAccessReader>,
    cache: SstCache,
}

impl SstFileReader {
    fn new(
        desc: &TableDesc,
        file: Box<dyn RandomAccessReader>,
        cache: Option<Arc<dyn Cache>>,
    ) -> SstFileReader {
        SstFileReader {
            desc: desc.clone(),
            file,
            cache: SstCache::new(cache),
        }
    }

    async fn read_block(&self, handle: &BlockHandle) -> Result<Arc<Vec<u8>>> {
        if let Some(block) = self.cache.get(self.desc.table_number, handle.offset).await {
            return Ok(block);
        }
        let block = self.file.read_at(handle.offset, handle.size).await?;
        let block = Arc::new(block);
        assert_eq!(block.len() as u64, handle.size);
        self.cache
            .put(self.desc.table_number, handle.offset, block.clone())
            .await;
        Ok(block)
    }
}

pub struct SstReader {
    file: Arc<SstFileReader>,
    index_block: Arc<Vec<u8>>,
}

impl SstReader {
    pub async fn open(
        desc: &TableDesc,
        file: Box<dyn RandomAccessReader>,
        cache: Option<Arc<dyn Cache>>,
    ) -> Result<SstReader> {
        assert!(desc.table_size >= FOOTER_SIZE);
        let data = file
            .read_at(desc.table_size - FOOTER_SIZE, FOOTER_SIZE)
            .await?;
        let footer = SstFooter::decode_from(&data);
        let reader = SstFileReader::new(desc, file, cache);
        let index_block = reader.read_block(&footer.index_handle).await?;
        Ok(SstReader {
            file: Arc::new(reader),
            index_block,
        })
    }

    fn new_internal_iterator(&self) -> TwoLevelIterator {
        let index_iter = BlockIterator::new(self.index_block.clone());
        let block_iter = SstBlockIterGenerator::new(self.file.clone());
        TwoLevelIterator::new(Box::new(index_iter), Box::new(block_iter))
    }
}

#[async_trait]
impl TableReader for SstReader {
    async fn get(&self, ts: Timestamp, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let mut iter = self.new_iterator();
        iter.seek(ts, key).await;
        if let Some(ent) = iter.current()? {
            if ent.0 <= ts && ent.1 == key {
                return Ok(Some(ent.2.to_owned()));
            }
        }
        Ok(None)
    }

    fn new_iterator(&self) -> Box<dyn Iterator> {
        Box::new(self.new_internal_iterator())
    }
}

pub struct SstBlockIterGenerator {
    file: Arc<SstFileReader>,
}

impl SstBlockIterGenerator {
    fn new(file: Arc<SstFileReader>) -> SstBlockIterGenerator {
        SstBlockIterGenerator { file }
    }
}

#[async_trait]
impl BlockIterGenerator for SstBlockIterGenerator {
    async fn spawn(&self, index_value: &[u8]) -> Result<Box<dyn Iterator>> {
        let block_handle = BlockHandle::decode_from(index_value);
        let block = self.file.read_block(&block_handle).await?;
        Ok(Box::new(BlockIterator::new(block)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        format::Entry,
        fs::{Fs, LocalFs},
    };

    #[tokio::test]
    async fn test() {
        const NUM: u64 = 1024;
        let fs = LocalFs::new("/tmp/engula_test").unwrap();
        let desc = {
            let options = SstOptions::default();
            let file = fs.new_sequential_writer("test.sst").await.unwrap();
            let mut builder = SstBuilder::new(options, file, 0);
            for i in 0..NUM {
                let v = i.to_be_bytes();
                builder.add(i, &v, &v).await;
            }
            builder.finish().await.unwrap()
        };
        let file = fs.new_random_access_reader("test.sst").await.unwrap();
        let reader = SstReader::open(&desc, file, None).await.unwrap();
        let mut iter = reader.new_iterator();
        iter.seek_to_first().await;
        for i in 0..NUM {
            let v = i.to_be_bytes();
            assert_eq!(iter.current().unwrap(), Some(Entry(i, &v, &v)));
            iter.next().await;
        }
        assert_eq!(iter.current().unwrap(), None);
        let ts = NUM / 2;
        let target = ts.to_be_bytes();
        iter.seek(ts, &target).await;
        assert_eq!(iter.current().unwrap(), Some(Entry(ts, &target, &target)));
        for i in 0..NUM {
            let expect = i.to_be_bytes();
            let actual = reader.get(i, &expect).await.unwrap().unwrap();
            assert_eq!(&actual, &expect);
            let actual = reader.get(i + 10, &expect).await.unwrap().unwrap();
            assert_eq!(&actual, &expect);
        }
    }
}
