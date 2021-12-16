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

//! An Engula engine that provides simple key-value storage.

mod codec;
mod engine;
mod error;
mod memtable;
mod table_builder;
mod table_reader;

pub use self::{
    engine::Engine,
    error::{Error, Result},
};

#[cfg(test)]
mod tests {
    use engula_kernel::mem::Kernel;
    use tokio::fs::OpenOptions;

    use crate::{table_builder::TableBuilder, table_reader::TableReader, *};

    #[tokio::test]
    async fn engine() -> Result<()> {
        const N: u32 = 4096;

        let kernel = Kernel::open().await?;
        let engine = Engine::open(kernel.clone()).await?;
        for i in 0..N {
            let k = i.to_be_bytes().to_vec();
            engine.put(k.clone(), k.clone()).await?;
            let got = engine.get(&k).await?;
            assert_eq!(got, Some(k.clone()));
            if i % 2 == 0 {
                engine.delete(k.clone()).await?;
                let got = engine.get(&k).await?;
                assert_eq!(got, None);
            }
        }

        // Re-open
        let engine = Engine::open(kernel.clone()).await?;
        for i in 0..N {
            let k = i.to_be_bytes().to_vec();
            if i % 2 == 0 {
                let got = engine.get(&k).await?;
                assert_eq!(got, None);
            } else {
                let got = engine.get(&k).await?;
                assert_eq!(got, Some(k))
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn table() -> Result<()> {
        let records = vec![
            (vec![1], Some(vec![1])),
            (vec![2], Some(vec![2])),
            (vec![3], None),
        ];

        let path = std::env::temp_dir().join("table");
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .await?;
        let mut builder = TableBuilder::new(file);
        for record in &records {
            builder.add(&record.0, &record.1).await?;
        }
        builder.finish().await?;

        let file = OpenOptions::new().read(true).open(&path).await?;
        let reader = TableReader::new(file).await?;
        for record in &records {
            let got = reader.get(&record.0).await?;
            assert_eq!(got.as_ref(), Some(&record.1));
        }

        Ok(())
    }
}
