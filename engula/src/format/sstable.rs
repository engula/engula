use std::sync::Arc;

use async_trait::async_trait;
use bytes::BufMut;

use super::{
    block::{BlockBuilder, BlockHandle, BlockIterator, BLOCK_HANDLE_SIZE},
    iterator::Iterator,
    table::{TableBuilder, TableReader, TableReaderOptions},
    two_level_iterator::{IterGenerator, TwoLevelIterator},
    TableDesc, Timestamp,
};
use crate::{
    cache::Cache,
    error::Result,
    fs::{RandomAccessReader, SequentialWriter},
};

#[derive(Clone, Debug)]
pub struct SstableOptions {
    pub block_size: usize,
}

impl SstableOptions {
    pub fn default() -> SstableOptions {
        SstableOptions {
            block_size: 16 * 1024,
        }
    }
}

pub const FOOTER_SIZE: u64 = BLOCK_HANDLE_SIZE;

struct Footer {
    index_handle: BlockHandle,
}

impl Footer {
    fn decode_from(buf: &[u8]) -> Footer {
        Footer {
            index_handle: BlockHandle::decode_from(buf),
        }
    }

    fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&self.index_handle.encode());
        buf
    }
}

pub struct SstableBuilder {
    options: SstableOptions,
    done: bool,
    writer: BlockWriter,
    last_ts: Timestamp,
    last_key: Vec<u8>,
    data_block: BlockBuilder,
    index_block: BlockBuilder,
}

impl SstableBuilder {
    pub fn new(
        options: SstableOptions,
        table_writer: Box<dyn SequentialWriter>,
        table_number: u64,
    ) -> SstableBuilder {
        SstableBuilder {
            options,
            done: false,
            writer: BlockWriter::new(table_writer, table_number),
            last_ts: 0,
            last_key: Vec::new(),
            data_block: BlockBuilder::new(),
            index_block: BlockBuilder::new(),
        }
    }

    async fn flush_data_block(&mut self) {
        let block = self.data_block.finish();
        let block_handle = self.writer.write_block(block).await;
        self.index_block
            .add(self.last_ts, &self.last_key, &block_handle.encode());
        self.data_block.reset();
    }
}

#[async_trait]
impl TableBuilder for SstableBuilder {
    async fn add(&mut self, ts: Timestamp, key: &[u8], value: &[u8]) {
        assert!(!self.done);
        let this_key = key.to_owned();
        assert!(this_key > self.last_key || (this_key == self.last_key && ts < self.last_ts));
        self.last_ts = ts;
        self.last_key = this_key;
        self.data_block.add(ts, key, value);
        if self.data_block.approximate_size() >= self.options.block_size {
            self.flush_data_block().await;
        }
    }

    async fn finish(&mut self) -> Result<TableDesc> {
        assert!(!self.done);
        self.done = true;
        if self.data_block.approximate_size() > 0 {
            self.flush_data_block().await;
        }
        let index_block = self.index_block.finish();
        let index_handle = self.writer.write_block(index_block).await;
        let footer = Footer { index_handle };
        let _ = self.writer.write_block(&footer.encode()).await;
        self.writer.finish().await
    }
}

pub struct SstableReader {
    reader: Arc<BlockReader>,
    index_block: Arc<Vec<u8>>,
}

impl SstableReader {
    pub async fn new(
        file: Box<dyn RandomAccessReader>,
        desc: TableDesc,
        options: TableReaderOptions,
    ) -> Result<SstableReader> {
        assert!(desc.sstable_size >= FOOTER_SIZE);
        let data = file
            .read_at(desc.sstable_size - FOOTER_SIZE, FOOTER_SIZE)
            .await?;
        let footer = Footer::decode_from(&data);
        let reader = BlockReader::new(file, desc, options).await?;
        let index_block = reader.read_block(&footer.index_handle).await?;
        Ok(SstableReader {
            reader: Arc::new(reader),
            index_block,
        })
    }

    fn new_internal_iterator(&self) -> TwoLevelIterator {
        let index_iter = BlockIterator::new(self.index_block.clone());
        let block_iter = BlockIterGenerator::new(self.reader.clone());
        TwoLevelIterator::new(Box::new(index_iter), Box::new(block_iter))
    }
}

#[async_trait]
impl TableReader for SstableReader {
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

struct BlockWriter {
    file: Box<dyn SequentialWriter>,
    number: u64,
    offset: u64,
    buffer: Vec<u8>,
}

impl BlockWriter {
    fn new(file: Box<dyn SequentialWriter>, number: u64) -> BlockWriter {
        let buffer = Vec::with_capacity(file.suggest_buffer_size());
        BlockWriter {
            file,
            number,
            offset: 0,
            buffer,
        }
    }

    async fn write_block(&mut self, block: &[u8]) -> BlockHandle {
        let handle = BlockHandle {
            offset: self.offset,
            size: block.len() as u64,
        };
        self.offset += block.len() as u64;
        self.buffer.extend_from_slice(block);
        if self.buffer.len() >= self.file.suggest_buffer_size() {
            self.file.write(self.buffer.split_off(0)).await;
        }
        handle
    }

    async fn finish(&mut self) -> Result<TableDesc> {
        if !self.buffer.is_empty() {
            self.file.write(self.buffer.split_off(0)).await;
        }
        self.file.finish().await?;
        Ok(TableDesc {
            table_number: self.number,
            sstable_size: self.offset,
            ..Default::default()
        })
    }
}

struct BlockReader {
    file: Box<dyn RandomAccessReader>,
    desc: TableDesc,
    cache: BlockCache,
    prefetch_buffer: Vec<u8>,
}

impl BlockReader {
    async fn new(
        file: Box<dyn RandomAccessReader>,
        desc: TableDesc,
        options: TableReaderOptions,
    ) -> Result<BlockReader> {
        let cache = BlockCache::new(options.cache);
        let prefetch_buffer = if options.prefetch {
            file.read_at(0, desc.sstable_size).await?
        } else {
            Vec::new()
        };
        Ok(BlockReader {
            file,
            desc,
            cache,
            prefetch_buffer,
        })
    }

    async fn read_block(&self, handle: &BlockHandle) -> Result<Arc<Vec<u8>>> {
        if let Some(block) = self.cache.get(self.desc.table_number, handle.offset).await {
            return Ok(block);
        }

        let start = handle.offset as usize;
        let limit = (handle.offset + handle.size) as usize;
        if self.prefetch_buffer.len() >= limit {
            let block = self.prefetch_buffer[start..limit].to_vec();
            return Ok(Arc::new(block));
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

const CACHE_KEY_LEN: usize = 16;

fn make_cache_key(number: u64, offset: u64) -> [u8; CACHE_KEY_LEN] {
    let mut buf = [0; CACHE_KEY_LEN];
    let mut pos = buf.as_mut();
    pos.put_u64_le(number);
    pos.put_u64_le(offset);
    buf
}

struct BlockCache {
    cache: Option<Arc<dyn Cache>>,
}

impl BlockCache {
    fn new(cache: Option<Arc<dyn Cache>>) -> BlockCache {
        BlockCache { cache }
    }

    async fn get(&self, number: u64, offset: u64) -> Option<Arc<Vec<u8>>> {
        if let Some(cache) = self.cache.as_ref() {
            let key = make_cache_key(number, offset);
            cache.get(&key).await
        } else {
            None
        }
    }

    async fn put(&self, number: u64, offset: u64, value: Arc<Vec<u8>>) {
        if let Some(cache) = self.cache.as_ref() {
            let key = make_cache_key(number, offset);
            cache.put(key.to_vec(), value).await;
        }
    }
}

pub struct BlockIterGenerator {
    reader: Arc<BlockReader>,
}

impl BlockIterGenerator {
    fn new(reader: Arc<BlockReader>) -> BlockIterGenerator {
        BlockIterGenerator { reader }
    }
}

#[async_trait]
impl IterGenerator for BlockIterGenerator {
    async fn spawn(&self, index_value: &[u8]) -> Result<Box<dyn Iterator>> {
        let block_handle = BlockHandle::decode_from(index_value);
        let block = self.reader.read_block(&block_handle).await?;
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
        let fs = LocalFs::new("/tmp/engula_test/sstable").unwrap();
        let desc = {
            let file = fs.new_sequential_writer("test.sst").await.unwrap();
            let mut builder = SstableBuilder::new(SstableOptions::default(), file, 0);
            for i in 0..NUM {
                let v = i.to_be_bytes();
                builder.add(i, &v, &v).await;
            }
            builder.finish().await.unwrap()
        };
        let file = fs.new_random_access_reader("test.sst").await.unwrap();
        let reader = SstableReader::new(file, desc, TableReaderOptions::default())
            .await
            .unwrap();
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
