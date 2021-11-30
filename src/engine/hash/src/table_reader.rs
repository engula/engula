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

use std::{collections::HashMap, io::ErrorKind};

use tokio::io::{AsyncRead, AsyncReadExt};

use crate::Result;

pub struct TableReader {
    map: HashMap<Vec<u8>, Vec<u8>>,
}

#[allow(dead_code)]
impl TableReader {
    pub async fn new<R: AsyncRead + Unpin>(mut read: R) -> Result<TableReader> {
        let map = read_all(&mut read).await?;
        Ok(TableReader { map })
    }

    pub async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(self.map.get(key).cloned())
    }
}

type IoResult<T> = std::result::Result<T, std::io::Error>;

async fn read_all<R: AsyncRead + Unpin>(read: &mut R) -> IoResult<HashMap<Vec<u8>, Vec<u8>>> {
    let mut map = HashMap::new();
    loop {
        match read_one(read).await {
            Ok(record) => {
                assert!(map.insert(record.0, record.1).is_none());
            }
            Err(err) => {
                if err.kind() == ErrorKind::UnexpectedEof {
                    return Ok(map);
                } else {
                    return Err(err);
                }
            }
        }
    }
}

async fn read_one<R: AsyncRead + Unpin>(read: &mut R) -> IoResult<(Vec<u8>, Vec<u8>)> {
    let klen = read.read_u64().await?;
    let mut key = vec![0; klen as usize];
    read.read_exact(&mut key).await?;
    let vlen = read.read_u64().await?;
    let mut value = vec![0; vlen as usize];
    read.read_exact(&mut value).await?;
    Ok((key, value))
}
