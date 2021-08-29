use bytes::BufMut;

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
}

impl BlockBuilder {
    pub fn new() -> BlockBuilder {
        BlockBuilder { buf: Vec::new() }
    }

    pub fn reset(&mut self) {
        self.buf.clear();
    }

    pub fn add(&mut self, ts: Timestamp, key: &[u8], value: &[u8]) {
        self.buf.put_u64_le(ts);
        self.buf.put_u32_le(key.len() as u32);
        self.buf.put_u32_le(value.len() as u32);
        self.buf.put_slice(key);
        self.buf.put_slice(value);
    }

    pub fn finish(&self) -> &[u8] {
        &self.buf
    }

    pub fn approximate_size(&self) -> usize {
        self.buf.len()
    }
}
