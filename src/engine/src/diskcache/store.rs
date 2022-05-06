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
    collections::VecDeque,
    io::Result,
    path::{Path, PathBuf},
};

use bytes::{Buf, BufMut};
use monoio::fs::{File, OpenOptions};

use super::DiskOptions;

struct ActiveFile {
    path: PathBuf,
    file: File,
    offset: u32,
    buffer: Vec<u8>,
    buffer_size: usize,
}

impl ActiveFile {
    async fn open(path: impl Into<PathBuf>, buffer_size: usize) -> Result<ActiveFile> {
        let path = path.into();
        let file = OpenOptions::new().create(true).open(&path).await?;
        Ok(Self {
            path,
            file,
            offset: 0,
            buffer: Vec::with_capacity(buffer_size),
            buffer_size,
        })
    }

    fn size(&self) -> usize {
        self.offset as usize + self.buffer.len()
    }

    async fn seal(mut self) -> Result<SealedFile> {
        if !self.buffer.is_empty() {
            self.flush().await?;
        }
        Ok(SealedFile {
            path: self.path,
            file: self.file,
            size: self.offset as usize,
        })
    }

    async fn read(&self, offset: u32, length: u32) -> Result<Vec<u8>> {
        if offset < self.offset {
            assert!(offset + length <= self.offset);
            let buf = Vec::with_capacity(length as usize);
            let (res, buf) = self.file.read_exact_at(buf, offset as u64).await;
            res.map(|_| buf)
        } else {
            let start = (offset - self.offset) as usize;
            let end = start + length as usize;
            assert!(end <= self.buffer.len());
            Ok(self.buffer[start..end].to_vec())
        }
    }

    async fn write(&mut self, key: &[u8], value: &[u8]) -> Result<(u32, u32)> {
        let offset = self.size() as u32;
        let length = encode_object(&mut self.buffer, key, value);
        if self.buffer.len() >= self.buffer_size {
            self.flush().await?;
        }
        Ok((offset, length))
    }

    async fn flush(&mut self) -> Result<()> {
        let buf = std::mem::take(&mut self.buffer);
        let (res, mut buf) = self.file.write_all_at(buf, self.offset as u64).await;
        std::mem::swap(&mut self.buffer, &mut buf);
        if res.is_ok() {
            self.offset += self.buffer.len() as u32;
            self.buffer.clear();
        }
        res
    }
}

struct SealedFile {
    path: PathBuf,
    file: File,
    size: usize,
}

impl SealedFile {
    fn size(&self) -> usize {
        self.size
    }

    async fn read(&self, offset: u32, length: u32) -> Result<Vec<u8>> {
        let buf = Vec::with_capacity(length as usize);
        let (res, buf) = self.file.read_exact_at(buf, offset as u64).await;
        res.map(|_| buf)
    }

    fn remove(self) -> Result<()> {
        // TODO: uses async remove
        std::fs::remove_file(self.path)
    }
}

pub struct DiskStore {
    root: PathBuf,
    options: DiskOptions,
    store_size: usize,
    active_file: ActiveFile,
    sealed_files: VecDeque<SealedFile>,
    active_fileno: u32,
    oldest_fileno: u32,
}

pub struct BlockHandle {
    pub fileno: u32,
    pub offset: u32,
    pub length: u32,
}

impl DiskStore {
    pub async fn open(root: PathBuf, options: DiskOptions) -> Result<DiskStore> {
        // TODO: recovers from existing files
        let active_fileno = 1;
        let active_path = file_path(&root, active_fileno);
        let active_file = ActiveFile::open(active_path, options.write_buffer_size).await?;
        Ok(Self {
            root,
            options,
            store_size: 0,
            active_file,
            sealed_files: VecDeque::new(),
            active_fileno,
            oldest_fileno: active_fileno,
        })
    }

    pub async fn read(&self, key: &[u8], handle: &BlockHandle) -> Result<Option<Vec<u8>>> {
        assert!(handle.fileno <= self.active_fileno);
        if handle.fileno == self.active_fileno {
            let buf = self.active_file.read(handle.offset, handle.length).await?;
            Ok(Some(buf))
        } else if handle.fileno < self.oldest_fileno {
            Ok(None)
        } else {
            let pos = handle.fileno - self.oldest_fileno;
            let buf = self.sealed_files[pos as usize]
                .read(handle.offset, handle.length)
                .await?;
            Ok(parse_block(&buf, key))
        }
    }

    pub async fn write(&mut self, key: &[u8], value: &[u8]) -> Result<BlockHandle> {
        let (offset, length) = self.active_file.write(key, value).await?;
        let handle = BlockHandle {
            fileno: self.active_fileno,
            offset,
            length,
        };
        if self.active_file.size() >= self.options.file_size {
            self.next_file().await?;
            self.evict_files().await?;
        }
        Ok(handle)
    }

    async fn next_file(&mut self) -> Result<()> {
        // TODO: handles fileno overflow
        self.active_fileno += 1;
        let next_path = file_path(&self.root, self.active_fileno);
        let next_file = ActiveFile::open(next_path, self.options.write_buffer_size).await?;
        let active_file = std::mem::replace(&mut self.active_file, next_file);
        let sealed_file = active_file.seal().await?;
        self.store_size += sealed_file.size();
        self.sealed_files.push_back(sealed_file);
        Ok(())
    }

    async fn evict_files(&mut self) -> Result<()> {
        while self.store_size + self.options.file_size >= self.options.disk_capacity {
            if let Some(file) = self.sealed_files.pop_back() {
                self.store_size -= file.size();
                file.remove()?;
                self.oldest_fileno += 1;
            } else {
                break;
            }
        }
        Ok(())
    }
}

fn file_path(root: impl AsRef<Path>, fileno: u32) -> PathBuf {
    root.as_ref().join(format!("{}", fileno))
}

fn parse_block(block: &[u8], target: &[u8]) -> Option<Vec<u8>> {
    let (key, value) = decode_object(block);
    if key == target {
        Some(value.to_owned())
    } else {
        None
    }
}

fn encode_object(mut buf: &mut [u8], key: &[u8], value: &[u8]) -> u32 {
    buf.put_u32(key.len() as u32);
    buf.put_u32(value.len() as u32);
    buf.put(key);
    buf.put(value);
    (8 + key.len() + value.len()) as u32
}

fn decode_object(mut buf: &[u8]) -> (&[u8], &[u8]) {
    let klen = buf.get_u32() as usize;
    let vlen = buf.get_u32() as usize;
    let key = &buf[..klen];
    buf.advance(klen);
    let value = &buf[..vlen];
    (key, value)
}
