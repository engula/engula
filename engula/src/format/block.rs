use bytes::{Buf, BufMut};

use super::iterator::*;
use crate::common::Timestamp;

pub struct BlockHandle {
    pub offset: u64,
    pub size: u64,
}

impl BlockHandle {
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
            restarts: Vec::new(),
            restart_interval: 16,
        }
    }

    pub fn reset(&mut self) {
        self.buf.clear();
        self.count = 0;
        self.restarts.clear();
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
    new_offset.map(|x| *x = offset + 16 + klen + vlen);
    (ts, &data[0..klen], &data[klen..(klen + vlen)])
}

pub struct BlockIterator<'a> {
    data: &'a [u8],
    num_restarts: usize,
    num_restarts_offset: usize,
    restart_start: usize,
    current_offset: usize,
}

impl<'a> BlockIterator<'a> {
    fn new(data: &[u8]) -> BlockIterator {
        let num_restarts_offset = data.len() - 4;
        let mut num_restarts_data = &data[num_restarts_offset..];
        let num_restarts = num_restarts_data.get_u32_le() as usize;
        let restart_start = num_restarts_offset - num_restarts * 4;
        BlockIterator {
            data,
            num_restarts,
            num_restarts_offset,
            restart_start,
            current_offset: num_restarts_offset,
        }
    }

    fn decode_restart_offset(&self, index: usize) -> usize {
        assert!(index < self.num_restarts);
        let offset = self.restart_start + index * 4;
        let mut restarts_data = &self.data[offset..];
        restarts_data.get_u32_le() as usize
    }
}

impl<'a> Iterator for BlockIterator<'a> {
    fn valid(&self) -> bool {
        self.current_offset < self.num_restarts_offset
    }

    fn seek_to_first(&mut self) {
        self.current_offset = 0;
    }

    fn seek(&mut self, ts: Timestamp, target: &[u8]) {
        let mut left = 0;
        let mut right = self.num_restarts - 1;
        while left < right {
            let mid = (left + right + 1) / 2;
            let offset = self.decode_restart_offset(mid);
            let version = decode_version(self.data, offset, None);
            if version.1 < target || version.1 == target && version.0 > ts {
                left = mid;
            } else {
                right = mid - 1;
            }
        }

        self.current_offset = self.decode_restart_offset(left);
        loop {
            if let Some(version) = self.current() {
                if version.1 > target || version.1 == target && version.0 < ts {
                    break;
                }
            } else {
                break;
            }
            self.next();
        }
    }

    fn next(&mut self) {
        if self.current_offset < self.num_restarts_offset {
            decode_version(
                self.data,
                self.current_offset,
                Some(&mut self.current_offset),
            );
        }
    }

    fn current(&self) -> Option<Version> {
        if self.valid() {
            Some(decode_version(self.data, self.current_offset, None))
        } else {
            None
        }
    }
}
