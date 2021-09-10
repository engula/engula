use std::{
    future::Future,
    pin::Pin,
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

pub struct BatchReceiver {
    rx: mpsc::Receiver<Write>,
    batch_size: usize,
}

impl BatchReceiver {
    pub fn new(rx: mpsc::Receiver<Write>, batch_size: usize) -> BatchReceiver {
        BatchReceiver { rx, batch_size }
    }

    pub async fn recv(&mut self) -> Option<Vec<Write>> {
        self.await
    }
}

impl Future for BatchReceiver {
    type Output = Option<Vec<Write>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut output = Vec::new();
        let mut output_size = 0;
        loop {
            match self.rx.poll_recv(cx) {
                Poll::Ready(Some(v)) => {
                    output_size += v.encode_size();
                    output.push(v);
                    if output_size >= self.batch_size {
                        return Poll::Ready(Some(output));
                    }
                }
                Poll::Ready(None) => {
                    if output.is_empty() {
                        return Poll::Ready(None);
                    } else {
                        return Poll::Ready(Some(output));
                    }
                }
                Poll::Pending => {
                    if output.is_empty() {
                        return Poll::Pending;
                    } else {
                        return Poll::Ready(Some(output));
                    }
                }
            }
        }
    }
}
