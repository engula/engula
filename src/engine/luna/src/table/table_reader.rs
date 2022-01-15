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

use engula_futures::io::{RandomRead, RandomReadExt};

use crate::{
    table::{
        block_handle::BlockHandle,
        block_reader::{read_block, BlockReader},
        table_iter::TableIter,
    },
    Result,
};

pub struct TableReader<R> {
    // For simplicity, the R is used here instead of its reference.
    reader: R,
    index_block: BlockReader,
}

#[allow(dead_code)]
impl<R: RandomRead + Unpin> TableReader<R> {
    pub async fn open(reader: R, size: usize) -> Result<Self> {
        let mut footer = [0; core::mem::size_of::<BlockHandle>()];
        let offset = size - core::mem::size_of::<BlockHandle>();
        reader.read_exact(&mut footer, offset).await?;
        let index_block_handler = BlockHandle::decode_from(&mut footer.as_slice());
        let block_content = read_block(&reader, &index_block_handler).await?;

        Ok(TableReader {
            reader,
            index_block: BlockReader::new(block_content),
        })
    }

    pub fn iter(&self) -> TableIter<R> {
        let index_iter = self.index_block.iter();
        TableIter::new(index_iter, &self.reader)
    }

    pub async fn get(&self, target: &[u8]) -> Result<Option<Vec<u8>>> {
        let mut iter = self.iter();
        iter.seek(target).await?;
        if iter.valid() && iter.key() == target {
            Ok(Some(iter.value().into()))
        } else {
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::table::table_builder::{TableBuilder, TableBuilderOptions};

    async fn build_table(size: u8) -> Arc<[u8]> {
        let opt = TableBuilderOptions {
            block_restart_interval: 2,
            ..Default::default()
        };
        let mut buf = vec![];
        let cursor = std::io::Cursor::new(&mut buf);
        let mut builder = TableBuilder::new(opt, cursor);
        for id in 1u8..size {
            builder.add(&[id], &[id]).await.unwrap();
        }
        builder.finish().await.unwrap();
        buf.into()
    }

    fn match_key(key_opt: &Option<Vec<u8>>, expect: &Option<Vec<u8>>) {
        match &expect {
            Some(v) => assert!(matches!(key_opt, Some(x) if x == v)),
            None => assert!(matches!(key_opt, None)),
        }
    }

    #[tokio::test]
    async fn seek_to_first() {
        let data = build_table(128).await;
        let reader = TableReader::open(&*data, data.len()).await.unwrap();
        let mut it = reader.iter();
        it.seek_to_first().await.unwrap();
        assert!(it.valid());
        assert_eq!(it.key(), vec![1u8]);
    }

    #[tokio::test]
    async fn seek() {
        struct Test {
            target: Vec<u8>,
            expect: Option<Vec<u8>>,
        }
        let tests: Vec<Test> = vec![
            // 1. out of range
            Test {
                target: vec![0u8],
                expect: Some(vec![1u8]),
            },
            Test {
                target: vec![129u8],
                expect: None,
            },
            // 2. binary search boundary
            Test {
                target: vec![1u8],
                expect: Some(vec![1u8]),
            },
            Test {
                target: vec![128u8],
                expect: Some(vec![128u8]),
            },
            // 3. binary search
            Test {
                target: vec![64u8],
                expect: Some(vec![64u8]),
            },
            Test {
                target: vec![65u8],
                expect: Some(vec![65u8]),
            },
            Test {
                target: vec![66u8],
                expect: Some(vec![66u8]),
            },
        ];

        let data = build_table(129).await;
        let reader = TableReader::open(&*data, data.len()).await.unwrap();
        let mut it = reader.iter();
        for t in tests {
            it.seek(&t.target).await.unwrap();
            let key_opt = if it.valid() {
                Some(it.key().to_owned())
            } else {
                None
            };
            match_key(&key_opt, &t.expect);
        }
    }

    #[tokio::test]
    async fn next() {
        struct Test {
            target: Vec<u8>,
            expect: Option<Vec<u8>>,
        }
        let tests: Vec<Test> = vec![
            // 1. out of range
            Test {
                target: vec![0u8],
                expect: Some(vec![2u8]),
            },
            Test {
                target: vec![129u8],
                expect: None,
            },
            // 2. binary search boundary
            Test {
                target: vec![1u8],
                expect: Some(vec![2u8]),
            },
            Test {
                target: vec![128u8],
                expect: None,
            },
            // 3. binary search
            Test {
                target: vec![64u8],
                expect: Some(vec![65u8]),
            },
            Test {
                target: vec![65u8],
                expect: Some(vec![66u8]),
            },
            Test {
                target: vec![66u8],
                expect: Some(vec![67u8]),
            },
            Test {
                target: vec![127u8],
                expect: Some(vec![128u8]),
            },
        ];

        let data = build_table(129).await;
        let reader = TableReader::open(&*data, data.len()).await.unwrap();
        let mut it = reader.iter();
        for t in tests {
            it.seek(&t.target).await.unwrap();
            let key_opt = if it.valid() {
                it.next().await.unwrap();
                if it.valid() {
                    Some(it.key().to_owned())
                } else {
                    None
                }
            } else {
                None
            };
            match_key(&key_opt, &t.expect);
        }
    }

    #[tokio::test]
    async fn get() {
        struct Test {
            target: Vec<u8>,
            expect: Option<Vec<u8>>,
        }
        let tests: Vec<Test> = vec![
            // 1. out of range
            Test {
                target: vec![0u8],
                expect: None,
            },
            Test {
                target: vec![129u8],
                expect: None,
            },
            // 2. binary search boundary
            Test {
                target: vec![1u8],
                expect: Some(vec![1u8]),
            },
            Test {
                target: vec![128u8],
                expect: Some(vec![128u8]),
            },
        ];

        let data = build_table(129).await;
        let reader = TableReader::open(&*data, data.len()).await.unwrap();
        for t in tests {
            let key_opt = reader.get(&t.target).await.unwrap();
            match_key(&key_opt, &t.expect);
        }
        for i in 1u8..128u8 {
            let key = vec![i];
            let value_opt = Some(vec![i]);
            let key_opt = reader.get(&key).await.unwrap();
            match_key(&key_opt, &value_opt);
        }
    }
}
