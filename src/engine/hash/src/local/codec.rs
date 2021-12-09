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

use std::io::{Error as IoError, ErrorKind};

use bytes::{Buf, BufMut};
use tokio::io::{AsyncRead, AsyncReadExt};

use crate::{Error, Result};

pub type Timestamp = u64;
pub type Value = Option<Vec<u8>>;

#[repr(u8)]
enum ValueKind {
    Some = 0,
    None = 1,
    Unknown = 255,
}

impl From<ValueKind> for u8 {
    fn from(v: ValueKind) -> Self {
        v as u8
    }
}

impl From<u8> for ValueKind {
    fn from(v: u8) -> Self {
        match v {
            0 => ValueKind::Some,
            1 => ValueKind::None,
            _ => ValueKind::Unknown,
        }
    }
}

pub fn record_size(key: &[u8], value: &Value) -> usize {
    let klen = 8 + key.len();
    let vlen = match value {
        Some(value) => 8 + value.len(),
        None => 0,
    };
    1 + klen + vlen
}

pub fn put_record(buf: &mut impl BufMut, key: &[u8], value: &Value) {
    match value {
        Some(value) => {
            buf.put_u8(ValueKind::Some.into());
            buf.put_u64(key.len() as u64);
            buf.put_u64(value.len() as u64);
            buf.put_slice(key);
            buf.put_slice(value);
        }
        None => {
            buf.put_u8(ValueKind::None.into());
            buf.put_u64(key.len() as u64);
            buf.put_slice(key);
        }
    }
}

pub fn encode_record(key: &[u8], value: &Value) -> Vec<u8> {
    let cap = record_size(key, value);
    let mut buf = Vec::with_capacity(cap);
    put_record(&mut buf, key, value);
    buf
}

pub fn decode_record(mut buf: &[u8]) -> Result<(Vec<u8>, Value)> {
    if buf.len() < 9 {
        return Err(Error::corrupted("record size too small"));
    }
    let kind = buf.get_u8();
    let klen = buf.get_u64() as usize;
    match ValueKind::from(kind) {
        ValueKind::Some => {
            let vlen = buf.get_u64() as usize;
            if buf.len() < klen + vlen {
                Err(Error::corrupted("record size too small"))
            } else {
                Ok((
                    buf[0..klen].to_vec(),
                    Some(buf[klen..(klen + vlen)].to_vec()),
                ))
            }
        }
        ValueKind::None => {
            if buf.len() < klen {
                Err(Error::corrupted("record size too small"))
            } else {
                Ok((buf[0..klen].to_vec(), None))
            }
        }
        ValueKind::Unknown => Err(Error::corrupted(format!("invalid value kind {}", kind))),
    }
}

type IoResult<T> = std::result::Result<T, IoError>;

pub async fn read_record<R: AsyncRead + Unpin>(r: &mut R) -> IoResult<(Vec<u8>, Value)> {
    let kind = r.read_u8().await?;
    let klen = r.read_u64().await?;

    match ValueKind::from(kind) {
        ValueKind::Some => {
            let vlen = r.read_u64().await?;
            let mut key = vec![0; klen as usize];
            r.read_exact(&mut key).await?;
            let mut value = vec![0; vlen as usize];
            r.read_exact(&mut value).await?;
            Ok((key, Some(value)))
        }
        ValueKind::None => {
            let mut key = vec![0; klen as usize];
            r.read_exact(&mut key).await?;
            Ok((key, None))
        }
        ValueKind::Unknown => Err(IoError::new(
            ErrorKind::InvalidData,
            format!("invalid value kind {}", kind),
        )),
    }
}
