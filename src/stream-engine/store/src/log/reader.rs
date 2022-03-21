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
use std::{
    fs::File,
    io::{ErrorKind, Read, Seek, SeekFrom},
};

use super::format::*;
use crate::{Error, Result};

pub(crate) struct Reader {
    log_number: u64,
    file: File,

    checksum: bool,
    eof: bool,

    consumed_bytes: usize,
    next_record_offset: usize,

    buf_start: usize,
    buf_size: usize,
    buffer: Box<[u8; MAX_BLOCK_SIZE]>,
}

impl Reader {
    pub fn new(mut file: File, log_number: u64, checksum: bool) -> Result<Reader> {
        file.seek(SeekFrom::Start(0))?;
        Ok(Reader {
            log_number,
            file,
            checksum,
            eof: false,
            consumed_bytes: 0,
            next_record_offset: 0,
            buf_start: 0,
            buf_size: 0,
            buffer: Box::new([0u8; MAX_BLOCK_SIZE]),
        })
    }

    /// Returns the physical offset of the next of last record returned by
    /// [`read_record`]. Undefined before the first call of [`read_record`].
    pub fn next_record_offset(&self) -> usize {
        self.next_record_offset
    }

    pub fn read_record(&mut self) -> Result<Option<Vec<u8>>> {
        let mut in_fragmented_record = false;
        let mut content = vec![];
        loop {
            let (kind, record) = match self.read_physical_record()? {
                Some((k, r)) => (k, r),
                None => {
                    return Ok(None);
                }
            };
            match kind {
                RECORD_FULL => {
                    if in_fragmented_record {
                        return Err(Error::Corruption(
                            "partial record without end(1)".to_string(),
                        ));
                    }
                    self.next_record_offset = self.consumed_bytes + self.buf_start;
                    return Ok(Some(record));
                }
                RECORD_HEAD => {
                    if in_fragmented_record {
                        return Err(Error::Corruption(
                            "partial record without end(2)".to_string(),
                        ));
                    }
                    in_fragmented_record = true;
                    content.extend_from_slice(&record);
                }
                RECORD_MID => {
                    if !in_fragmented_record {
                        return Err(Error::Corruption(
                            "missing header of fragmented record(1)".to_string(),
                        ));
                    }
                    content.extend_from_slice(&record);
                }
                RECORD_TAIL => {
                    if !in_fragmented_record {
                        return Err(Error::Corruption(
                            "missing header of fragmented record(2)".to_string(),
                        ));
                    }
                    content.extend_from_slice(&record);
                    self.next_record_offset = self.consumed_bytes + self.buf_start;
                    return Ok(Some(content));
                }
                RECORD_BAD_LENGTH | RECORD_BAD_CRC32 => {
                    return Err(Error::Corruption(format!(
                        "fragmented {}, type {}",
                        in_fragmented_record, kind
                    )));
                }
                _ => {
                    return Err(Error::Corruption(format!("unknown record type {}", kind)));
                }
            }
        }
    }

    /// Reads next physical record from file, returns it's kind and content.
    ///
    /// A [`None`] is returned if the end of file is reached.
    ///
    /// Note that there might exists some zero record, who propuse is to align
    /// block or page. This function will skip those record directly.
    fn read_physical_record(&mut self) -> Result<Option<(u8, Vec<u8>)>> {
        loop {
            if self.buf_size <= RECORD_HEADER_SIZE {
                if !self.eof {
                    // Skip the trailing padding, if buf_size isn't zero.
                    self.buf_size = 0;
                    self.buf_start = 0;
                    self.consumed_bytes += MAX_BLOCK_SIZE;
                    self.read_block()?;
                    continue;
                } else {
                    // We have meet a truncated header in end of file.
                    self.buf_size = 0;
                    return Ok(None);
                }
            }

            let mut buf = &self.buffer[self.buf_start..];
            let kind = buf[0];
            if kind == RECORD_EMPTY {
                // This is a emptry record which no any valid content until the end of block. It
                // indicates that all records are consumed.
                return Ok(None);
            }

            if kind == RECORD_PAGE_ALIGN {
                // Whether this record is from previous log number or the current log number, it
                // is definitely safe to skip the remainder until the next page boundary.
                let next_page_boundary = (self.buf_start + PAGE_SIZE) & !(PAGE_SIZE - 1);
                if self.buf_start + self.buf_size < next_page_boundary {
                    // It is guaranteed to read a block if end of file isn't reached, that means the
                    // buf is always page aligned. So we could think the end of file is reached in
                    // this case.
                    debug_assert!(self.eof);
                    return Ok(None);
                }
                self.buf_size -= next_page_boundary - self.buf_start;
                self.buf_start = next_page_boundary;
                continue;
            }

            debug_assert!(RECORD_HEAD <= kind && kind <= RECORD_ZERO);

            let log_number = buf[1];
            assert!(log_number <= self.log_number as u8);
            if log_number < self.log_number as u8 {
                // This is a recycled log file, now all records are consumed.
                return Ok(None);
            }

            let size = u16::from_le_bytes(buf[2..4].try_into().unwrap()) as usize;
            let crc32 = u32::from_le_bytes(buf[4..8].try_into().unwrap());
            buf = &buf[RECORD_HEADER_SIZE..];

            if size + RECORD_HEADER_SIZE > self.buf_size {
                if !self.eof {
                    return Ok(Some((RECORD_BAD_LENGTH, vec![])));
                } else {
                    // The end of file has been reached, but no enough content are reading, assume
                    // the writer died in middle of writing the record.
                    return Ok(None);
                }
            }

            let content = &buf[..size];
            self.buf_start += size + RECORD_HEADER_SIZE;
            self.buf_size -= size + RECORD_HEADER_SIZE;
            if kind == RECORD_ZERO {
                continue;
            }

            if self.checksum && crc32fast::hash(content) != crc32 {
                return Ok(Some((RECORD_BAD_CRC32, vec![])));
            }

            return Ok(Some((kind, content.to_owned())));
        }
    }

    /// Reads a block from file, if there no enough data to read, the [`eof`] is
    /// set to [`true`].
    fn read_block(&mut self) -> Result<()> {
        debug_assert_eq!(self.buf_start, 0);
        while self.buf_size < MAX_BLOCK_SIZE {
            let read_size = match self.file.read(&mut self.buffer[self.buf_size..]) {
                Ok(size) => size,
                Err(err) => {
                    if err.kind() == ErrorKind::Interrupted {
                        continue;
                    }
                    return Err(err.into());
                }
            };
            if read_size == 0 {
                self.eof = true;
                break;
            }

            self.buf_size += read_size;
        }
        Ok(())
    }
}
