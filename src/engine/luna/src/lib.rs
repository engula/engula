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

mod collection;
mod database;
mod error;
mod table;

pub use self::{
    collection::Collection,
    database::{Database, Txn},
    error::{Error, Result},
};

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn database() -> Result<()> {
        let db = Database::open().await?;
        let co = db.create_collection("abc").await?;

        let k1 = vec![1];
        let k2 = vec![2];

        let mut txn = db.txn().await?;
        txn.put(&co, k1.clone(), k1.clone());
        txn.put(&co, k2.clone(), k2.clone());
        txn.commit().await?;

        let txn = db.txn().await?;
        let got = txn.get(&co, &k1).await?;
        assert_eq!(got.as_ref(), Some(&k1));
        let got = txn.get(&co, &k2).await?;
        assert_eq!(got.as_ref(), Some(&k2));

        let mut txn = db.txn().await?;
        txn.put(&co, k1.clone(), k2.clone());
        txn.delete(&co, k2.clone());
        txn.commit().await?;

        let txn = db.txn().await?;
        let got = txn.get(&co, &k1).await?;
        assert_eq!(got.as_ref(), Some(&k2));
        let got = txn.get(&co, &k2).await?;
        assert_eq!(got.as_ref(), None);

        Ok(())
    }
}
