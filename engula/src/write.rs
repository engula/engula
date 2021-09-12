use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

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

pub struct WriteReceiver {
    rx: mpsc::Receiver<Write>,
    batch_size: usize,
    output: Vec<Write>,
    output_size: usize,
}

impl WriteReceiver {
    pub fn new(rx: mpsc::Receiver<Write>, batch_size: usize) -> WriteReceiver {
        WriteReceiver {
            rx,
            batch_size,
            output: Vec::new(),
            output_size: 0,
        }
    }

    pub async fn recv(&mut self) -> Option<Vec<Write>> {
        self.await
    }

    fn take(&mut self) -> Vec<Write> {
        self.output_size = 0;
        self.output.split_off(0)
    }
}

impl Future for WriteReceiver {
    type Output = Option<Vec<Write>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        loop {
            match self.rx.poll_recv(cx) {
                Poll::Ready(Some(v)) => {
                    self.output_size += v.encode_size();
                    self.output.push(v);
                    if self.output_size >= self.batch_size {
                        return Poll::Ready(Some(self.take()));
                    }
                }
                Poll::Ready(None) => {
                    if self.output.is_empty() {
                        return Poll::Ready(None);
                    } else {
                        return Poll::Ready(Some(self.take()));
                    }
                }
                Poll::Pending => {
                    if self.output.is_empty() {
                        return Poll::Pending;
                    } else {
                        return Poll::Ready(Some(self.take()));
                    }
                }
            }
        }
    }
}

#[derive(Debug)]
pub struct WriteBatch {
    pub tx: Arc<mpsc::Sender<Vec<Write>>>,
    pub writes: Vec<Write>,
    pub buffer: Vec<u8>,
}

#[derive(Debug)]
pub struct WriteBuffer {
    pub writes: Vec<WriteBatch>,
    pub buffer: Vec<u8>,
}

impl WriteBuffer {
    fn new() -> WriteBuffer {
        WriteBuffer {
            writes: Vec::new(),
            buffer: Vec::new(),
        }
    }
}

pub struct WriteBatchReceiver {
    rx: mpsc::Receiver<WriteBatch>,
    batch_size: usize,
    output: WriteBuffer,
}

impl WriteBatchReceiver {
    pub fn new(rx: mpsc::Receiver<WriteBatch>, batch_size: usize) -> WriteBatchReceiver {
        WriteBatchReceiver {
            rx,
            batch_size,
            output: WriteBuffer::new(),
        }
    }

    pub async fn recv(&mut self) -> Option<WriteBuffer> {
        self.await
    }

    fn take(&mut self) -> WriteBuffer {
        WriteBuffer {
            writes: self.output.writes.split_off(0),
            buffer: self.output.buffer.split_off(0),
        }
    }
}

impl Future for WriteBatchReceiver {
    type Output = Option<WriteBuffer>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        loop {
            match self.rx.poll_recv(cx) {
                Poll::Ready(Some(mut v)) => {
                    self.output.buffer.append(&mut v.buffer);
                    self.output.writes.push(v);
                    if self.output.buffer.len() >= self.batch_size {
                        return Poll::Ready(Some(self.take()));
                    }
                }
                Poll::Ready(None) => {
                    if self.output.buffer.is_empty() {
                        return Poll::Ready(None);
                    } else {
                        return Poll::Ready(Some(self.take()));
                    }
                }
                Poll::Pending => {
                    if self.output.buffer.is_empty() {
                        return Poll::Pending;
                    } else {
                        return Poll::Ready(Some(self.take()));
                    }
                }
            }
        }
    }
}
