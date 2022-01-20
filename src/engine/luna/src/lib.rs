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
mod flush_scheduler;
mod level;
mod memtable;
mod merging_scanner;
mod options;
mod scan;
mod store;
mod table;
mod version;
mod write_batch;

pub(crate) const DEFAULT_NAME: &str = "default";

pub use self::{
    database::Database,
    error::{Error, Result},
    options::{Options, ReadOptions, Snapshot, WriteOptions},
    write_batch::WriteBatch,
};

#[cfg(test)]
mod tests {
    use engula_kernel::MemKernel;

    use super::*;

    #[tokio::test]
    async fn test() {
        let opts = Options {
            memtable_size: 1024,
        };
        let kernel = MemKernel::open().await.unwrap();
        let db = Database::open(opts, kernel).await.unwrap();
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

        let ropts = ReadOptions { snapshot };
        assert_eq!(db.get(&ropts, &k1).await.unwrap(), None);

        // Writes more data to test flushes.
        for i in 0..1024u64 {
            let k = i.to_be_bytes().to_vec();
            let mut wb = WriteBatch::default();
            wb.put(&k, &k);
            db.write(&wopts, wb).await.unwrap();
        }
    }
}
