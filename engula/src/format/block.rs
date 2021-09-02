use std::sync::Arc;

use async_trait::async_trait;
use bytes::{Buf, BufMut};

use super::iterator::*;
use crate::common::Timestamp;
use crate::error::Error;

pub const BLOCK_HANDLE_SIZE: usize = 16;

pub struct BlockHandle {
    pub offset: u64,
    pub size: u64,
}

impl BlockHandle {
    pub fn decode_from(mut buf: &[u8]) -> BlockHandle {
        BlockHandle {
            offset: buf.get_u64_le(),
            size: buf.get_u64_le(),
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.put_u64_le(self.offset);
        buf.put_u64_le(self.size);
        buf
    }
}

pub struct BlockBuilder {
    buf: Vec<u8>,
    count: usize,
    restarts: Vec<u32>,
    restart_interval: usize,
}

impl BlockBuilder {
    pub fn new() -> BlockBuilder {
        BlockBuilder {
            buf: Vec::new(),
            count: 0,
            restarts: vec![0],
            restart_interval: 16,
        }
    }

    pub fn reset(&mut self) {
        self.buf.clear();
        self.count = 0;
        self.restarts.clear();
        self.restarts.push(0);
    }

    pub fn add(&mut self, ts: Timestamp, key: &[u8], value: &[u8]) {
        if self.count >= self.restart_interval {
            self.restarts.push(self.buf.len() as u32);
            self.count = 0;
        }
        self.buf.put_u64_le(ts);
        self.buf.put_u32_le(key.len() as u32);
        self.buf.put_u32_le(value.len() as u32);
        self.buf.put_slice(key);
        self.buf.put_slice(value);
        self.count += 1;
    }

    pub fn finish(&mut self) -> &[u8] {
        for restart in &self.restarts {
            self.buf.put_u32_le(*restart);
        }
        self.buf.put_u32_le(self.restarts.len() as u32);
        &self.buf
    }

    pub fn approximate_size(&self) -> usize {
        self.buf.len()
    }
}

fn decode_version<'a>(
    data: &'a [u8],
    offset: usize,
    new_offset: Option<&mut usize>,
) -> Version<'a> {
    let mut data = &data[offset..];
    let ts = data.get_u64_le();
    let klen = data.get_u32_le() as usize;
    let vlen = data.get_u32_le() as usize;
    if let Some(x) = new_offset {
        *x = offset + 16 + klen + vlen
    }
    Version(ts, &data[0..klen], &data[klen..(klen + vlen)])
}

#[derive(Debug)]
pub struct BlockIterator {
    data: Arc<Vec<u8>>,
    num_restarts: usize,
    num_restarts_offset: usize,
    restart_offset: usize,
    current_offset: usize,
}

impl BlockIterator {
    pub fn new(data: Arc<Vec<u8>>) -> BlockIterator {
        let num_restarts_offset = data.len() - 4;
        let mut num_restarts_data = &data[num_restarts_offset..];
        let num_restarts = num_restarts_data.get_u32_le() as usize;
        let restart_offset = num_restarts_offset - num_restarts * 4;
        BlockIterator {
            data,
            num_restarts,
            num_restarts_offset,
            restart_offset,
            current_offset: restart_offset,
        }
    }

    fn decode_restart_offset(&self, index: usize) -> usize {
        assert!(index < self.num_restarts);
        let offset = self.restart_offset + index * 4;
        let mut restarts_data = &self.data[offset..];
        restarts_data.get_u32_le() as usize
    }
}

#[async_trait]
impl Iterator for BlockIterator {
    fn valid(&self) -> bool {
        self.current_offset < self.restart_offset
    }

    fn error(&self) -> Option<Error> {
        None
    }

    async fn seek_to_first(&mut self) {
        self.current_offset = 0;
    }

    async fn seek(&mut self, ts: Timestamp, key: &[u8]) {
        let target = Version(ts, key, &[]);
        let mut left = 0;
        let mut right = self.num_restarts - 1;
        while left < right {
            let mid = (left + right + 1) / 2;
            let offset = self.decode_restart_offset(mid);
            let version = decode_version(&self.data, offset, None);
            if version <= target {
                left = mid;
            } else {
                right = mid - 1;
            }
        }

        self.current_offset = self.decode_restart_offset(left);

        while let Some(version) = self.current() {
            if version >= target {
                break;
            }
            self.next().await;
        }
    }

    async fn next(&mut self) {
        if self.current_offset < self.num_restarts_offset {
            decode_version(
                &self.data,
                self.current_offset,
                Some(&mut self.current_offset),
            );
        }
    }

    fn current(&self) -> Option<Version> {
        if self.valid() {
            Some(decode_version(&self.data, self.current_offset, None))
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test() {
        let mut builder = BlockBuilder::new();
        let versions: [(Timestamp, &[u8], &[u8]); 5] = [
            (3, &[1], &[3]),
            (1, &[1], &[1]),
            (4, &[2], &[4]),
            (2, &[2], &[2]),
            (5, &[5], &[5]),
        ];
        for v in &versions {
            builder.add(v.0, v.1, v.2);
        }
        let block = builder.finish().to_owned();
        let mut iter = BlockIterator::new(Arc::new(block));
        assert!(!iter.valid());
        iter.seek_to_first().await;
        for v in versions {
            assert!(iter.valid());
            assert_eq!(iter.current(), Some(v.into()));
            iter.next().await;
        }
        assert_eq!(iter.current(), None);
        iter.seek(3, &[2]).await;
        assert_eq!(iter.current(), Some(Version(2, [2].as_ref(), [2].as_ref())));
        iter.next().await;
        assert_eq!(iter.current(), Some(Version(5, [5].as_ref(), [5].as_ref())));
    }
}
