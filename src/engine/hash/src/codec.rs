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

#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    Put(Vec<u8>),
    Deletion,
}

const PUT_VALUE_KIND: u8 = 0;
const DELETION_VALUE_KIND: u8 = 1;

pub fn record_size(key: &[u8], value: &Value) -> usize {
    let klen = 8 + key.len();
    let vlen = match value {
        Value::Put(value) => 8 + value.len(),
        Value::Deletion => 0,
    };
    1 + klen + vlen
}

pub fn put_record(buf: &mut impl BufMut, key: &[u8], value: &Value) {
    match value {
        Value::Put(value) => {
            buf.put_u8(PUT_VALUE_KIND);
            buf.put_u64(key.len() as u64);
            buf.put_u64(value.len() as u64);
            buf.put_slice(key);
            buf.put_slice(value);
        }
        Value::Deletion => {
            buf.put_u8(DELETION_VALUE_KIND);
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
    if kind == PUT_VALUE_KIND {
        let vlen = buf.get_u64() as usize;
        if buf.len() < klen + vlen {
            return Err(Error::corrupted("record size too small"));
        }
        Ok((
            buf[0..klen].to_vec(),
            Value::Put(buf[klen..(klen + vlen)].to_vec()),
        ))
    } else if kind == DELETION_VALUE_KIND {
        if buf.len() < klen {
            return Err(Error::corrupted("record size too small"));
        }
        Ok((buf[0..klen].to_vec(), Value::Deletion))
    } else {
        Err(Error::corrupted(format!("invalid value kind {}", kind)))
    }
}

type IoResult<T> = std::result::Result<T, IoError>;

pub async fn read_record<R: AsyncRead + Unpin>(r: &mut R) -> IoResult<(Vec<u8>, Value)> {
    let kind = r.read_u8().await?;
    let klen = r.read_u64().await?;
    if kind == PUT_VALUE_KIND {
        let vlen = r.read_u64().await?;
        let mut key = vec![0; klen as usize];
        r.read_exact(&mut key).await?;
        let mut value = vec![0; vlen as usize];
        r.read_exact(&mut value).await?;
        Ok((key, Value::Put(value)))
    } else if kind == DELETION_VALUE_KIND {
        let mut key = vec![0; klen as usize];
        r.read_exact(&mut key).await?;
        Ok((key, Value::Deletion))
    } else {
        Err(IoError::new(
            ErrorKind::InvalidData,
            format!("invalid value kind {}", kind),
        ))
    }
}
