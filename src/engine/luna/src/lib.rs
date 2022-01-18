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

mod codec;
mod database;
mod error;
mod mem_table;
mod merging_scanner;
mod scan;
mod table;
mod version;
mod write_batch;

pub use self::{
    database::{Database, ReadOptions, WriteOptions},
    error::{Error, Result},
    write_batch::WriteBatch,
};

#[cfg(test)]
mod tests {
    use engula_kernel::MemKernel;

    use super::*;

    #[tokio::test]
    async fn test() {
        let kernel = MemKernel::open().await.unwrap();
        let db = Database::open(kernel).await.unwrap();
        let ropts = ReadOptions::default();
        let wopts = WriteOptions::default();
        let snapshot = db.snapshot().await;

        let mut wb = WriteBatch::default();
        let k1 = vec![1];
        let k2 = vec![2];
        wb.put(&k1, &k1).put(&k2, &k2).delete(&k2);
        db.write(&wopts, wb).await.unwrap();

        assert_eq!(db.get(&ropts, &k1).await.unwrap(), Some(k1.clone()));
        assert_eq!(db.get(&ropts, &k2).await.unwrap(), None);

        let ropts = ReadOptions {
            snapshot: Some(snapshot),
        };
        assert_eq!(db.get(&ropts, &k1).await.unwrap(), None);
    }
}
