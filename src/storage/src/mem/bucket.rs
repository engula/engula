// Copyright 2021 The Engula Authors.
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

use std::{
    collections::{hash_map, HashMap},
    future::Future,
    io::{Error as IoError, ErrorKind, IoSlice},
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    sync::Mutex,
};

use crate::{async_trait, Error, Result};

#[derive(Clone)]
pub struct Bucket {
    inner: Arc<Mutex<Inner>>,
}

struct Inner {
    objects: HashMap<String, Arc<Vec<u8>>>,
}

impl Default for Bucket {
    fn default() -> Self {
        let inner = Inner {
            objects: HashMap::new(),
        };
        Self {
            inner: Arc::new(Mutex::new(inner)),
        }
    }
}

/// An interface to manipulate a bucket.
#[async_trait]
impl crate::Bucket for Bucket {
    type SequentialReader = SequentialReader;
    type SequentialWriter = SequentialWriter;

    async fn delete_object(&self, name: &str) -> Result<()> {
        let mut inner = self.inner.lock().await;
        match inner.objects.remove(name) {
            Some(_) => Ok(()),
            None => Err(Error::NotFound(format!("object '{}'", name))),
        }
    }

    async fn new_sequential_reader(&self, name: &str) -> Result<Self::SequentialReader> {
        let inner = self.inner.lock().await;
        match inner.objects.get(name) {
            Some(object) => Ok(SequentialReader::new(object.clone())),
            None => Err(Error::NotFound(format!("object '{}'", name))),
        }
    }

    async fn new_sequential_writer(&self, name: &str) -> Result<Self::SequentialWriter> {
        Ok(SequentialWriter::new(name.to_owned(), self.inner.clone()))
    }
}

pub struct SequentialReader {
    buf: Arc<Vec<u8>>,
    pos: usize,
}

impl SequentialReader {
    fn new(buf: Arc<Vec<u8>>) -> Self {
        Self { buf, pos: 0 }
    }
}

impl AsyncRead for SequentialReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<IoResult<()>> {
        let end = std::cmp::min(self.buf.len(), self.pos + buf.remaining());
        buf.put_slice(&self.buf[self.pos..end]);
        self.pos = end;
        Poll::Ready(Ok(()))
    }
}

pub struct SequentialWriter {
    name: String,
    data: Vec<u8>,
    inner: Arc<Mutex<Inner>>,
}

impl SequentialWriter {
    fn new(name: String, inner: Arc<Mutex<Inner>>) -> Self {
        Self {
            name,
            data: Vec::new(),
            inner,
        }
    }
}

type IoResult<T> = std::result::Result<T, IoError>;

impl AsyncWrite for SequentialWriter {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<IoResult<usize>> {
        Pin::new(&mut self.data).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<IoResult<()>> {
        Pin::new(&mut self.data).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<IoResult<()>> {
        if Pin::new(&mut self.data).poll_shutdown(cx).is_ready() {
            let data = self.data.split_off(0);
            let mut lock = Box::pin(self.inner.lock());
            if let Poll::Ready(mut inner) = lock.as_mut().poll(cx) {
                match inner.objects.entry(self.name.clone()) {
                    hash_map::Entry::Vacant(ent) => {
                        ent.insert(Arc::new(data));
                        return Poll::Ready(Ok(()));
                    }
                    hash_map::Entry::Occupied(ent) => {
                        let err = IoError::new(
                            ErrorKind::AlreadyExists,
                            format!("object '{}'", ent.key()),
                        );
                        return Poll::Ready(Err(err));
                    }
                }
            }
        }
        Poll::Pending
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<IoResult<usize>> {
        Pin::new(&mut self.data).poll_write_vectored(cx, bufs)
    }

    fn is_write_vectored(&self) -> bool {
        self.data.is_write_vectored()
    }
}
