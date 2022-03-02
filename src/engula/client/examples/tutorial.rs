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

use anyhow::Result;
use engula_client::{Any, Blob, List, Map, Universe, I64};

#[tokio::main]
async fn main() -> Result<()> {
    // The address of the server you started above.
    let url = "http://localhost:21716";
    let uv = Universe::connect(url).await?;
    let db = uv.create_database("db").await?;
    let c1 = db.create_collection::<Any>("c1").await?;
    let c2 = db.create_collection::<I64>("c2").await?;
    let c3 = db.create_collection::<Blob>("c3").await?;
    let c4 = db.create_collection::<List<Any>>("c4").await?;
    let c5 = db.create_collection::<List<I64>>("c5").await?;
    let c6 = db.create_collection::<List<Blob>>("c6").await?;
    let c7 = db.create_collection::<Map<Any>>("c7").await?;
    let c8 = db.create_collection::<Map<I64>>("c8").await?;
    let c9 = db.create_collection::<Map<Blob>>("c9").await?;

    // Manipulate collections
    {
        // Sets the Any object with i64 (I64)
        c1.set("o", 1).await?;
        // Sets the Any object with Vec<u8> (Blob)
        c1.set("o", vec![1u8, 2u8]).await?;
        // Sets the Any object with Vec<i64> (List<I64>)
        c1.set("o", vec![1i64, 2i64]).await?;
        // Gets and prints the object
        println!("c1.o = {:?}", c1.get("o").await?);
        // Deletes the object
        c1.delete("o").await?;
        // Sets the I64 object with i64
        c2.set("o", 1).await?;
        println!("c2.o = {:?}", c2.get("o").await?);
        // Sets the Blob object with Vec<u8>
        c3.set("o", vec![1, 2]).await?;
        println!("c3.o = {:?}", c3.get("o").await?);
        // Sets the List<Any> object with Vec<i64>
        c4.set("o", vec![1.into(), 2.into()]).await?;
        println!("c4.o = {:?}", c4.get("o").await?);
        // Sets the List<I64> object with Vec<i64>
        c5.set("o", vec![1, 2]).await?;
        println!("c5.o = {:?}", c5.get("o").await?);
        // Sets the List<Blob> object with Vec<Vec<u8>>
        c6.set("o", vec![vec![1], vec![2]]).await?;
        println!("c6.o = {:?}", c6.get("o").await?);
        // Sets the Map<Any> object with HashMap<Vec<u8>, Value>
        c7.set("o", [(vec![1], 1.into()), (vec![2], vec![2u8].into())])
            .await?;
        println!("c7.o = {:?}", c7.get("o").await?);
        // Sets the Map<I64> object with HashMap<Vec<u8>, i64>
        c8.set("o", [(vec![1], 1), (vec![2], 2)]).await?;
        println!("c8.o = {:?}", c8.get("o").await?);
        // Sets the Map<Blob> object with HashMap<Vec<u8>, Vec<u8>>
        c9.set("o", [(vec![1], vec![1]), (vec![2], vec![2])])
            .await?;
        println!("c9.o = {:?}", c9.get("o").await?);
    }

    // Manipulate individual objects
    {
        // Any object
        c1.set("o", 1).await?;
        c1.object("o").add(1).await?;
        println!("c1.o = {:?}", c1.get("o").await?);
        // I64 object
        c2.object("o").add(2).await?;
        println!("c2.o = {:?}", c2.get("o").await?);
        // Blob object
        c3.object("o").append(vec![3u8, 4u8]).await?;
        println!("c3.o = {:?}", c3.get("o").await?);
        println!("c3.o.len = {:?}", c3.object("o").len().await?);
        // List<I64> object
        c5.object("o").push_back(3).await?;
        c5.object("o").push_front(0).await?;
        println!("c5.o = {:?}", c5.get("o").await?);
        println!("c5.o.len = {:?}", c5.object("o").len().await?);
        // Map<Blob> object
        c9.object("o").set(vec![3], vec![3]).await?;
        c9.object("o").delete(vec![1]).await?;
        println!("c9.o = {:?}", c9.get("o").await?);
        println!("c9.o.len = {:?}", c9.object("o").len().await?);
        println!("c9.o.[3] = {:?}", c9.object("o").get(vec![3]).await?);
    }

    // Object-level transactions
    {
        // Updates a List<I64> object in a transaction.
        let mut txn = c5.object("txn").begin();
        txn.push_back(1).push_front(0);
        txn.commit().await?;
        println!("c5.txn = {:?}", c5.get("txn").await?);
        // Updates a Map<Blob> object in a transaction.
        let mut txn = c9.object("txn").begin();
        txn.set(vec![1], vec![1])
            .set(vec![2], vec![2])
            .delete(vec![3]);
        txn.commit().await?;
        println!("c9.txn = {:?}", c9.get("txn").await?);
    }

    // Collection-level transactions
    {
        // Updates multiple I64 objects in a transaction.
        let mut txn = c2.begin();
        txn.set("a", 1);
        txn.object("b").add(1).sub(2);
        txn.commit().await?;
        // Updates multiple List<I64> objects in a transaction.
        let mut txn = c5.begin();
        txn.set("a", vec![1, 2]);
        txn.object("b").push_back(3).push_front(0);
        txn.commit().await?;
    }

    // Database-level transactions
    {
        // Begins a database transaction
        let txn = db.begin();
        {
            // Begins a collection sub-transaction
            let mut t = c5.begin_with(txn.clone());
            t.set("a", vec![1, 2]);
            t.object("b").push_back(3);
            // Commits the sub-transaction.
            // Note that the operations will not be executed yet.
            t.commit().await?;
        }
        {
            // Begins another collection sub-transaction
            let mut t = c9.begin_with(txn.clone());
            t.set("a", [(vec![1], vec![1]), (vec![2], vec![2])]);
            t.object("b").set(vec![3], vec![3]);
            t.commit().await?;
        }
        // Commits the database transaction and executes all the sub-transactions.
        // This will fail if there is any uncommitted sub-transaction.
        txn.commit().await?;
    }

    Ok(())
}
