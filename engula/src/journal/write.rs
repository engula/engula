use std::sync::Arc;

use tokio::sync::{mpsc, oneshot};

use crate::format::Timestamp;

#[derive(Debug)]
pub struct Write {
    pub tx: oneshot::Sender<()>,
    pub ts: Timestamp,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

impl Write {
    pub fn encode_to(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&self.ts.to_le_bytes());
        buf.extend_from_slice(&self.key);
        buf.extend_from_slice(&self.value);
    }

    pub fn encode_size(&self) -> usize {
        std::mem::size_of_val(&self.ts) + self.key.len() + self.value.len()
    }
}

#[derive(Debug)]
pub struct WriteBatch {
    pub tx: Arc<mpsc::Sender<Vec<Write>>>,
    pub buffer: Vec<u8>,
    pub writes: Vec<Write>,
}
