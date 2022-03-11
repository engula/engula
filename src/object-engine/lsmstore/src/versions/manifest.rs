// Copyright 2022 The Engula Authors.
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

use std::{io, sync::Arc};

use crc::{Crc, CRC_32_ISCSI};
use tokio::{
    fs,
    io::{AsyncReadExt, AsyncWriteExt},
    sync::Mutex,
};

use crate::*;

/// Log File format:
///
/// File is broken down into variable sized records. The format of each record
/// is described below.
///       +-----+-------------+--+----+----------+------+-- ... ----+
/// File  | r0  |        r1   |P | r2 |    r3    |  r4  |           |
///       +-----+-------------+--+----+----------+------+-- ... ----+
///       <--- BLOCK_SIZE ------>|<-- BLOCK_SIZE ------>|
///  rn = variable size records
///  P = Padding
///
/// Data is written out in BLOCK_SIZE chunks. If next record does not fit
/// into the space left, the leftover space will be padded with \0.
///
/// Record format(Rocksdb Legacy):
///
/// +---------+-----------+-----------+--- ... ---+
/// |CRC (4B) | Size (2B) | Type (1B) | Payload   |
/// +---------+-----------+-----------+--- ... ---+
///
/// CRC = 32bit hash computed over the record type and payload using CRC
/// Size = Length of the payload data
/// Type = Type of record
///        (CHUNK_TYPE_FULL, CHUNK_TYPE_FIRST, CHUNK_TYPE_LAST,
/// CHUNK_TYPE_MIDDLE )        The type is used to group a bunch of records
/// together to represent        blocks that are larger than BLOCK_SIZE
/// Payload = Byte stream as long as specified by the payload size

const BLOCK_SIZE: usize = 32 * 1024;
const HEADER_SIZE: usize = 7;
#[allow(dead_code)]
pub const MAX_FILE_SIZE: usize = 128 << 20;

pub const CRC: Crc<u32> = Crc::<u32>::new(&CRC_32_ISCSI);

const CHUNK_TYPE_FULL: u8 = 1;
const CHUNK_TYPE_FIRST: u8 = 2;
const CHUNK_TYPE_MIDDLE: u8 = 3;
const CHUNK_TYPE_LAST: u8 = 4;

pub struct Writer {
    inner: Arc<Mutex<WriterInner>>,
}

struct WriterInner {
    w: fs::File,
    buf: [u8; BLOCK_SIZE],
    pending: bool,
    first: bool,
    chunk_written: usize,
    written_block_cnt: u64,
    chunk_start: usize,
    chunk_end: usize,
}

impl Writer {
    pub fn new(w: fs::File) -> Self {
        Self {
            inner: Arc::new(Mutex::new(WriterInner {
                w,
                buf: [0u8; BLOCK_SIZE],
                pending: false,
                first: false,
                written_block_cnt: 0,
                chunk_start: 0,
                chunk_end: 0,
                chunk_written: 0,
            })),
        }
    }

    pub async fn append(&mut self, record: &[u8]) -> Result<()> {
        let mut inner = self.inner.lock().await;
        if inner.pending {
            inner.fill_header(true);
        }
        inner.chunk_start = inner.chunk_end;
        inner.chunk_end += HEADER_SIZE;
        if inner.chunk_end > BLOCK_SIZE {
            for k in inner.chunk_start..BLOCK_SIZE {
                inner.buf[k] = 0
            }
            inner.write_block().await?
        }
        inner.first = true;
        inner.pending = true;

        let mut buf = record;
        while !buf.is_empty() {
            if inner.chunk_end == BLOCK_SIZE {
                inner.fill_header(false);
                inner.write_block().await?;
                inner.first = false;
            }
            let pos = inner.chunk_end;
            let (src, remain) = if buf.len() < inner.buf[pos..].len() {
                inner.chunk_end += buf.len();
                (buf, &buf[buf.len()..])
            } else {
                inner.chunk_end += inner.buf[pos..].len();
                (
                    &buf[..inner.buf[pos..].len()],
                    &buf[inner.buf[pos..].len()..],
                )
            };
            inner.buf[pos..pos + src.len()].copy_from_slice(src);
            buf = remain;
        }

        Ok(())
    }

    pub async fn flush_and_sync(&mut self) -> Result<()> {
        let mut inner = self.inner.lock().await;
        inner.write_pending().await?;
        inner.w.flush().await?;
        inner.w.sync_all().await?;
        Ok(())
    }

    #[allow(dead_code)]
    pub async fn accumulated_size(&self) -> usize {
        let inner = self.inner.lock().await;
        inner.written_block_cnt as usize * BLOCK_SIZE + inner.chunk_end
    }
}

impl WriterInner {
    fn fill_header(&mut self, last: bool) {
        assert!(self.chunk_start + HEADER_SIZE <= self.chunk_end && self.chunk_end <= BLOCK_SIZE);
        let typ = if last {
            if self.first {
                CHUNK_TYPE_FULL
            } else {
                CHUNK_TYPE_LAST
            }
        } else if self.first {
            CHUNK_TYPE_FIRST
        } else {
            CHUNK_TYPE_MIDDLE
        };
        self.buf[self.chunk_start + 6] = typ;
        let crc = CRC.checksum(&self.buf[self.chunk_start + 6..self.chunk_end]);
        let data_size = (self.chunk_end - self.chunk_start - HEADER_SIZE) as u16;
        self.buf[self.chunk_start..self.chunk_start + 4].copy_from_slice(&crc.to_le_bytes());
        self.buf[self.chunk_start + 4..self.chunk_start + 6]
            .copy_from_slice(&data_size.to_le_bytes());
    }

    async fn write_block(&mut self) -> Result<()> {
        if self.pending {
            self.fill_header(true);
            self.pending = false;
        }
        self.w.write_all(&self.buf[self.chunk_written..]).await?;
        self.chunk_start = 0;
        self.chunk_end = HEADER_SIZE;
        self.chunk_written = 0;
        self.written_block_cnt += 1;
        Ok(())
    }

    async fn write_pending(&mut self) -> Result<()> {
        if self.pending {
            self.fill_header(true);
            self.pending = false;
        }
        self.w
            .write_all(&self.buf[self.chunk_written..self.chunk_end])
            .await?;
        self.chunk_written = self.chunk_end;
        Ok(())
    }
}

pub struct Reader {
    inner: Arc<Mutex<ReaderInner>>,
}

struct ReaderInner {
    r: fs::File,
    buf: [u8; BLOCK_SIZE],
    chunk_start: usize,
    chunk_end: usize,
    read_bytes: usize,
    last: bool,
    block_num: i64,
}

impl Reader {
    pub fn new(r: fs::File) -> Self {
        let buf = [0u8; BLOCK_SIZE];
        let inner = Arc::new(Mutex::new(ReaderInner {
            r,
            buf,
            chunk_start: 0,
            chunk_end: 0,
            read_bytes: 0,
            last: false,
            block_num: -1,
        }));
        Self { inner }
    }

    pub async fn read(&mut self) -> Result<Vec<u8>> {
        let mut inner = self.inner.lock().await;
        inner.chunk_start = inner.chunk_end;
        inner.next_chunk(true).await?;

        while inner.chunk_start == inner.chunk_end {
            if inner.last {
                return Err(Error::Io(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "EOF",
                )));
            }
            inner.next_chunk(false).await?;
        }
        let mut res = vec![0u8; inner.chunk_end - inner.chunk_start];
        res.copy_from_slice(&inner.buf[inner.chunk_start..inner.chunk_end]);
        inner.chunk_start = inner.chunk_end;

        Ok(res)
    }
}

impl ReaderInner {
    async fn next_chunk(&mut self, first_read: bool) -> Result<()> {
        loop {
            if self.chunk_end + HEADER_SIZE <= self.read_bytes {
                let checksum = u32::from_le_bytes(
                    self.buf[self.chunk_end..self.chunk_end + 4]
                        .try_into()
                        .unwrap(),
                );
                let data_size = u16::from_le_bytes(
                    self.buf[self.chunk_end + 4..self.chunk_end + 6]
                        .try_into()
                        .unwrap(),
                ) as usize;
                let typ = self.buf[self.chunk_end + 6];

                if checksum == 0 && data_size == 0 && typ == 0 {
                    return Err(Error::Corrupted("zeroed chunk".to_string()));
                }

                self.chunk_start = self.chunk_end + HEADER_SIZE;
                self.chunk_end = self.chunk_start + data_size;
                if self.chunk_end > self.read_bytes {
                    return Err(Error::Corrupted("invalid chunk".to_string()));
                }
                if checksum
                    != CRC.checksum(&self.buf[self.chunk_start - HEADER_SIZE + 6..self.chunk_end])
                {
                    return Err(Error::Corrupted("invalid chunk".to_string()));
                }
                if first_read && typ != CHUNK_TYPE_FULL && typ != CHUNK_TYPE_FIRST {
                    continue;
                }
                self.last = typ == CHUNK_TYPE_LAST || typ == CHUNK_TYPE_FULL;
                return Ok(());
            }

            if self.read_bytes < BLOCK_SIZE && self.block_num >= 0 {
                if !first_read || self.chunk_start != self.read_bytes {
                    return Err(Error::Corrupted("invalid chunk".to_string()));
                }
                return Err(Error::Io(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "EOF",
                )));
            }

            let readed = self.r.read(&mut self.buf[..]).await?;
            self.chunk_start = 0;
            self.chunk_end = 0;
            self.read_bytes = readed;
            self.block_num += 1;
        }
    }
}
