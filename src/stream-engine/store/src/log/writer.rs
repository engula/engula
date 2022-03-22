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
    io::{Error, ErrorKind, IoSlice, Seek, SeekFrom, Write},
};

use super::format::*;
use crate::{fs::FileExt, IoResult as Result};

const EMPTY_RECORD_HEADER: [u8; RECORD_HEADER_SIZE] = [0u8; RECORD_HEADER_SIZE];

pub(crate) struct Writer {
    log_number: u64,
    file: File,

    /// The number of block this file already used (exclusive the last partial
    /// writing block).
    num_block: usize,

    /// The offset of first avail byte in a block.
    block_offset: usize,

    /// The maximum allowed bytes of this file.
    max_file_size: usize,

    synced_offset: usize,
}

impl Writer {
    pub fn new(
        mut file: File,
        log_number: u64,
        initial_offset: usize,
        max_file_size: usize,
    ) -> Result<Writer> {
        let num_block = initial_offset / MAX_BLOCK_SIZE;
        let block_offset = initial_offset % MAX_BLOCK_SIZE;

        if initial_offset > max_file_size {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                format!(
                    "too large initial offset, limitation {}, but got {}",
                    max_file_size, initial_offset
                ),
            ));
        }

        let synced_offset = initial_offset - (initial_offset % PAGE_SIZE);
        file.seek(SeekFrom::Start(initial_offset as u64))?;
        let mut writer = Writer {
            log_number,
            file,
            num_block,
            block_offset,
            max_file_size,
            synced_offset,
        };

        // Skip trailing padding.
        if writer.block_offset + RECORD_HEADER_SIZE > MAX_BLOCK_SIZE {
            writer.switch_block(false)?;
        }

        Ok(writer)
    }

    /// Add bytes content into log file.
    ///
    /// REQUIRES: The returns value of [`avail_space()`] must be enough to hold
    /// the content.
    pub fn add_record(&mut self, content: &[u8]) -> Result<()> {
        debug_assert!(
            self.consumed_bytes() + RECORD_HEADER_SIZE + content.len() <= self.max_file_size,
            "a file must have avail space for new record, avail {}, max {}, slice {}",
            self.avail_space(),
            self.max_file_size,
            content.len()
        );

        let mut consumed = 0;
        loop {
            debug_assert!(
                self.block_offset + RECORD_HEADER_SIZE <= MAX_BLOCK_SIZE,
                "a block must have avail space for partial of record"
            );

            let free = MAX_BLOCK_SIZE - self.block_offset;
            let left = content.len() - consumed;
            let size = (free - RECORD_HEADER_SIZE).min(left);
            let payload = &content[consumed..(consumed + size)];
            let crc32 = crc32fast::hash(payload);
            let kind = if size == content.len() {
                RECORD_FULL
            } else if consumed == 0 {
                RECORD_HEAD
            } else if size + consumed == content.len() {
                RECORD_TAIL
            } else {
                RECORD_MID
            };

            // Only encode low 8-bits of the 64-bits log number, so maximum 255 logs are
            // recyclable.
            let mut header = Vec::with_capacity(RECORD_HEADER_SIZE);
            header.push(kind);
            header.push(self.log_number as u8);
            header.extend_from_slice(&(size as u16).to_le_bytes());
            header.extend_from_slice(&crc32.to_le_bytes());
            consumed += size;

            let slices = &mut [IoSlice::new(&header), IoSlice::new(payload)];
            self.file.write_all_vectored(slices)?;
            self.block_offset += RECORD_HEADER_SIZE + size;

            // Skip trailing padding.
            if RECORD_HEADER_SIZE + self.block_offset > MAX_BLOCK_SIZE {
                self.switch_block(true)?;
            }

            if kind == RECORD_TAIL || kind == RECORD_FULL {
                break;
            }
        }
        Ok(())
    }

    pub fn fill_entire_avail_space(&mut self) -> Result<()> {
        if self.block_offset > 0 {
            self.switch_block(true)?;
        }
        while self.num_block * MAX_BLOCK_SIZE < self.max_file_size {
            self.switch_block(true)?;
        }
        Ok(())
    }

    pub fn flush(&mut self) -> Result<()> {
        self.ensure_page_aligned()?;

        let offset = self.consumed_bytes();
        if self.synced_offset != offset {
            self.file.sync_data()?;
            self.synced_offset = offset;
        }

        Ok(())
    }

    #[inline(always)]
    pub fn log_number(&self) -> u64 {
        self.log_number
    }

    /// Returns the avail space, excluding record header.
    #[inline(always)]
    pub fn avail_space(&self) -> usize {
        (self.max_file_size - self.consumed_bytes()).saturating_sub(RECORD_HEADER_SIZE)
    }

    /// Returns the avail space of block, excluding record header.
    #[cfg(test)]
    #[inline(always)]
    pub fn block_avail_space(&self) -> usize {
        (MAX_BLOCK_SIZE - self.block_offset).saturating_sub(RECORD_HEADER_SIZE)
    }

    fn switch_block(&mut self, sync_data: bool) -> Result<()> {
        let avail = MAX_BLOCK_SIZE - self.block_offset;
        if avail < RECORD_HEADER_SIZE {
            self.file.write_all(&EMPTY_RECORD_HEADER[..avail])?;
        } else {
            self.add_zero_record(avail - RECORD_HEADER_SIZE)?;
        }

        self.block_offset = 0;
        self.num_block += 1;

        let size = self.num_block * MAX_BLOCK_SIZE;
        if sync_data && self.synced_offset + PAGE_SIZE <= size {
            let len = size - self.synced_offset;
            debug_assert_eq!(len % PAGE_SIZE, 0);
            self.file.sync_range(self.synced_offset, len)?;
            self.synced_offset += len;
        }
        Ok(())
    }

    /// Align the avail space up to page, to avoid partial write.
    fn ensure_page_aligned(&mut self) -> Result<()> {
        let avail = PAGE_SIZE - (self.consumed_bytes() % PAGE_SIZE);
        if 0 < avail && avail < PAGE_SIZE {
            let mut buf = vec![0u8; avail];
            buf[0] = RECORD_PAGE_ALIGN;
            self.file.write_all(&buf)?;
            self.block_offset += buf.len();
            if self.block_offset == MAX_BLOCK_SIZE {
                self.block_offset = 0;
                self.num_block += 1;
            }
        }
        Ok(())
    }

    /// Add a special zero record to current block.
    fn add_zero_record(&mut self, size: usize) -> Result<()> {
        debug_assert!(self.block_offset + RECORD_HEADER_SIZE + size <= MAX_BLOCK_SIZE);
        let mut buf = vec![0u8; RECORD_HEADER_SIZE + size];
        buf[0] = RECORD_ZERO;
        buf[1] = self.log_number as u8;
        buf[2..4].copy_from_slice(&(size as u16).to_le_bytes());

        self.file.write_all(&buf)?;
        self.block_offset += buf.len();

        Ok(())
    }

    /// Returns the bytes this file already appended.
    #[inline(always)]
    fn consumed_bytes(&self) -> usize {
        self.num_block * MAX_BLOCK_SIZE + self.block_offset
    }
}

impl Drop for Writer {
    fn drop(&mut self) {
        // Align the last block, so that reader would recognize the old records.
        if self.block_offset > 0 {
            if let Err(err) = self.switch_block(false) {
                tracing::error!("writer {} switch block: {}", self.log_number, err);
                return;
            }
        }

        if let Err(err) = self.flush() {
            tracing::error!("writer {} flush: {}", self.log_number, err);
        }
    }
}
