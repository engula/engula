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

use bytes::BufMut;
use tokio::io::{AsyncRead, AsyncReadExt};

pub type Timestamp = u64;

type IoResult<T> = std::result::Result<T, std::io::Error>;

pub fn record_size(key: &[u8], value: &[u8]) -> usize {
    16 + key.len() + value.len()
}

pub fn put_record<W: BufMut>(w: &mut W, key: &[u8], value: &[u8]) {
    w.put_u64(key.len() as u64);
    w.put_slice(key);
    w.put_u64(value.len() as u64);
    w.put_slice(value);
}

pub fn encode_record(key: &[u8], value: &[u8]) -> Vec<u8> {
    let cap = record_size(key, value);
    let mut buf = Vec::with_capacity(cap);
    put_record(&mut buf, key, value);
    buf
}

pub async fn read_record<R: AsyncRead + Unpin>(r: &mut R) -> IoResult<(Vec<u8>, Vec<u8>)> {
    let klen = r.read_u64().await?;
    let mut key = vec![0; klen as usize];
    r.read_exact(&mut key).await?;
    let vlen = r.read_u64().await?;
    let mut value = vec![0; vlen as usize];
    r.read_exact(&mut value).await?;
    Ok((key, value))
}
