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

mod base_version;
mod codec;
mod database;
mod error;
mod flush_scheduler;
mod level;
mod memtable;
mod merging_scanner;
mod options;
mod scan;
mod scanner;
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
    use std::sync::Arc;

    use engula_kernel::MemKernel;

    use super::*;

    #[tokio::test]
    async fn test() {
        let opts = Options::default();
        let kernel = Arc::new(MemKernel::open().await.unwrap());
        let db = Database::open(opts, kernel).await.unwrap();
        let ropts = ReadOptions::default();
        let wopts = WriteOptions::default();
        let snapshot = db.snapshot().await;

        let k1 = vec![1];
        let k2 = vec![2];
        let k3 = vec![3];
        let mut wb = WriteBatch::default();
        wb.put(&k1, &k1).put(&k2, &k2);
        db.write(&wopts, wb).await.unwrap();
        let mut wb = WriteBatch::default();
        wb.put(&k3, &k3).delete(&k2);
        db.write(&wopts, wb).await.unwrap();

        assert_eq!(db.get(&ropts, &k1).await.unwrap().as_ref(), Some(&k1));
        assert_eq!(db.get(&ropts, &k2).await.unwrap(), None);
        assert_eq!(db.get(&ropts, &k3).await.unwrap().as_ref(), Some(&k3));
        let mut scanner = db.scan(&ropts).await;
        scanner.seek_to_first().await.unwrap();
        assert!(scanner.valid());
        assert_eq!(scanner.key(), &k1);
        assert_eq!(scanner.value(), &k1);
        scanner.next().await.unwrap();
        assert!(scanner.valid());
        assert_eq!(scanner.key(), &k3);
        assert_eq!(scanner.value(), &k3);
        scanner.next().await.unwrap();
        assert!(!scanner.valid());

        let ropts = ReadOptions { snapshot };
        assert_eq!(db.get(&ropts, &k1).await.unwrap(), None);
        let mut scanner = db.scan(&ropts).await;
        scanner.seek_to_first().await.unwrap();
        assert!(!scanner.valid());
    }

    #[tokio::test]
    async fn flush() {
        let opts = Options {
            memtable_size: 1024,
        };
        let kernel = Arc::new(MemKernel::open().await.unwrap());
        let db = Database::open(opts, kernel).await.unwrap();
        let ropts = ReadOptions::default();
        let wopts = WriteOptions::default();

        let num = 256u64;
        for i in 0..num {
            let k = i.to_be_bytes().to_vec();
            let mut wb = WriteBatch::default();
            wb.put(&k, &k);
            db.write(&wopts, wb).await.unwrap();
            let mut scanner = db.scan(&ropts).await;
            scanner.seek_to_first().await.unwrap();
            for j in 0..=i {
                let k = j.to_be_bytes().to_vec();
                assert_eq!(db.get(&ropts, &k).await.unwrap().as_ref(), Some(&k));
                assert!(scanner.valid());
                assert_eq!(scanner.key(), &k);
                assert_eq!(scanner.value(), &k);
                scanner.next().await.unwrap();
            }
            assert!(!scanner.valid());
            scanner.seek(&(i / 2).to_be_bytes()).await.unwrap();
            for j in (i / 2)..=i {
                assert!(scanner.valid());
                assert_eq!(scanner.key(), &j.to_be_bytes());
                assert_eq!(scanner.value(), &j.to_be_bytes());
                scanner.next().await.unwrap();
            }
            assert!(!scanner.valid());
        }
    }

    #[tokio::test]
    async fn reopen() {
        let opts = Options {
            memtable_size: 1024,
        };
        let kernel = Arc::new(MemKernel::open().await.unwrap());
        let db = Database::open(opts.clone(), kernel.clone()).await.unwrap();
        let ropts = ReadOptions::default();
        let wopts = WriteOptions::default();

        let num = 256u64;
        for i in 0..num {
            let k = i.to_be_bytes().to_vec();
            let mut wb = WriteBatch::default();
            wb.put(&k, &k);
            db.write(&wopts, wb).await.unwrap();
            assert_eq!(db.get(&ropts, &k).await.unwrap().as_ref(), Some(&k));
        }

        // Re-open
        let db = Database::open(opts.clone(), kernel.clone()).await.unwrap();
        for i in 0..num {
            let k = i.to_be_bytes().to_vec();
            assert_eq!(db.get(&ropts, &k).await.unwrap().as_ref(), Some(&k));
        }
    }
}
