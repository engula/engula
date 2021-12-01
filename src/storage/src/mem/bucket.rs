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

type Object = Arc<Vec<u8>>;
type Objects = Arc<Mutex<HashMap<String, Object>>>;

#[derive(Clone)]
pub struct Bucket {
    objects: Objects,
}

impl Default for Bucket {
    fn default() -> Self {
        Self {
            objects: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

/// An interface to manipulate a bucket.
#[async_trait]
impl crate::Bucket for Bucket {
    type SequentialReader = SequentialReader;
    type SequentialWriter = SequentialWriter;

    async fn delete_object(&self, name: &str) -> Result<()> {
        let mut objects = self.objects.lock().await;
        match objects.remove(name) {
            Some(_) => Ok(()),
            None => Err(Error::NotFound(format!("object '{}'", name))),
        }
    }

    async fn new_sequential_reader(&self, name: &str) -> Result<Self::SequentialReader> {
        let objects = self.objects.lock().await;
        match objects.get(name) {
            Some(object) => Ok(SequentialReader::new(object.clone())),
            None => Err(Error::NotFound(format!("object '{}'", name))),
        }
    }

    async fn new_sequential_writer(&self, name: &str) -> Result<Self::SequentialWriter> {
        Ok(SequentialWriter::new(name.to_owned(), self.objects.clone()))
    }
}

pub struct SequentialReader {
    object: Object,
    offset: usize,
}

impl SequentialReader {
    fn new(object: Object) -> Self {
        Self { object, offset: 0 }
    }
}

impl AsyncRead for SequentialReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<IoResult<()>> {
        let start = self.offset;
        self.offset = std::cmp::min(self.object.len(), start + buf.remaining());
        buf.put_slice(&self.object[start..self.offset]);
        Poll::Ready(Ok(()))
    }
}

pub struct SequentialWriter {
    name: String,
    data: Vec<u8>,
    objects: Objects,
}

impl SequentialWriter {
    fn new(name: String, objects: Objects) -> Self {
        Self {
            name,
            data: Vec::new(),
            objects,
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
            let mut lock = Box::pin(self.objects.lock());
            if let Poll::Ready(mut objects) = lock.as_mut().poll(cx) {
                match objects.entry(self.name.clone()) {
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
