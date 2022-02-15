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
use engula_client::{Any, Blob, Int64, List, Map, Universe};

#[tokio::main]
async fn main() -> Result<()> {
    let url = "http://localhost:21716";
    let uv = Universe::connect(url).await?;
    let db = uv.database("Txn");
    let c0 = db.collection::<Any>("Any");
    let c1 = db.collection::<Blob>("Blob");
    let c2 = db.collection::<Int64>("Int64");
    let c3 = db.collection::<List<Blob>>("List<Blob>");
    let c4 = db.collection::<Map<Int64>>("Map<Int64>");

    let txn = db.begin();
    {
        println!("collection {}", c0.name());
        let mut c = c0.begin_with(txn.clone());
        c.object("blob").append(vec![1u8]).append(vec![2u8]);
        c.object("int64").add(1).sub(2);
        c.object("list").push_back(1).push_front(vec![1u8]);
        c.object("map").set(vec![1u8], 1).set(vec![2u8], "hello");
        c.commit().await?;
    }
    {
        println!("collection {}", c1.name());
        let mut c = c1.begin_with(txn.clone());
        c.object("a").append(vec![1, 2]).append(vec![3, 4]);
        c.object("b").append(vec![5, 6]).append(vec![7, 8]);
        c.commit().await?;
    }
    {
        println!("collection {}", c2.name());
        let mut c = c2.begin_with(txn.clone());
        c.object("a").add(1).sub(2);
        c.object("b").sub(1).add(2);
        c.commit().await?;
    }
    {
        println!("collection {}", c3.name());
        let mut c = c3.begin_with(txn.clone());
        c.object("a").push_back("hello").push_back("world");
        c.object("b").push_front("hello").push_front("world");
        c.commit().await?;
    }
    {
        println!("collection {}", c4.name());
        let mut c = c4.begin_with(txn.clone());
        c.object("a").set(vec![1], 1).set(vec![2], 2);
        c.object("b")
            .set(vec![3], 3)
            .set(vec![4], 4)
            .remove(vec![3]);
        c.commit().await?;
    }
    txn.commit().await?;

    {
        println!("c0[blob] = {:?}", c0.get("blob").await?);
        println!("c0[int64] = {:?}", c0.get("int64").await?);
        println!("c0[list] = {:?}", c0.get("list").await?);
        println!("c0[map] = {:?}", c0.get("map").await?);
        println!("c1[a] = {:?}", c1.get("a").await?);
        println!("c1[b] = {:?}", c1.get("b").await?);
        println!("c2[a] = {:?}", c2.get("a").await?);
        println!("c2[b] = {:?}", c2.get("b").await?);
        println!("c3[a] = {:?}", c3.get("a").await?);
        println!("c3[b] = {:?}", c3.get("b").await?);
        println!("c4[a] = {:?}", c4.get("a").await?);
        println!("c4[b] = {:?}", c4.get("b").await?);
    }

    Ok(())
}
