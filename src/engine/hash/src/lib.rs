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
    use tokio::fs::OpenOptions;

    use crate::{table_builder::TableBuilder, table_reader::TableReader, *};

    #[tokio::test]
    async fn engine() -> Result<()> {
        let engine = Engine::new();
        let key = vec![1];
        let value = vec![2];
        engine.set(key.clone(), value.clone()).await?;
        let got = engine.get(&key).await?;
        assert_eq!(got, Some(value));
        Ok(())
    }

    #[tokio::test]
    async fn table() -> Result<()> {
        let records = vec![(vec![1], vec![1]), (vec![2], vec![2])];

        let filename = "/tmp/hash";
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(filename)
            .await?;
        let mut builder = TableBuilder::new(file);
        for record in &records {
            builder.add(&record.0, &record.1).await?;
        }
        builder.finish().await?;

        let file = OpenOptions::new().read(true).open(filename).await?;
        let reader = TableReader::new(file).await?;
        for record in &records {
            let got = reader.get(&record.0).await?;
            assert_eq!(got.as_ref(), Some(&record.1));
        }

        Ok(())
    }
}
