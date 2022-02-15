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
use engula_client::{Any, Blob, Int64, List, Universe};

#[tokio::main]
async fn main() -> Result<()> {
    let url = "http://localhost:21716";
    let uv = Universe::connect(url).await?;
    let db = uv.database("db");

    {
        let c = db.collection::<List<Any>>("List<Any>");
        println!("collection {}", c.name());
        c.set("o", ["hello".into(), "world".into()]).await?;
        println!("o = {:?}", c.get("o").await?);
        println!("o.len = {:?}", c.object("o").len().await?);
        println!("o = {:?}", c.object("o").pop_back().await?);
        println!("o = {:?}", c.object("o").pop_front().await?);
        c.object("o").push_back("richard").await?;
        c.object("o").push_front("hello").await?;
        println!("o[1] = {:?}", c.object("o").get(1).await?);
        c.object("o").set(1, "world").await?;
        println!("o = {:?}", c.get("o").await?);
    }

    {
        let c = db.collection::<List<Blob>>("List<Blob>");
        println!("collection {}", c.name());
        c.set("o", [vec![1], vec![2]]).await?;
        println!("o = {:?}", c.get("o").await?);
        println!("o.len = {:?}", c.object("o").len().await?);
        println!("o = {:?}", c.object("o").pop_back().await?);
        println!("o = {:?}", c.object("o").pop_front().await?);
        c.object("o").push_back(vec![4]).await?;
        c.object("o").push_front(vec![3]).await?;
        println!("o[1] = {:?}", c.object("0").get(1).await?);
        println!("o = {:?}", c.get("o").await?);
    }

    {
        let c = db.collection::<List<Int64>>("List<Int64>");
        println!("collection {}", c.name());
        c.set("o", [1, 2]).await?;
        println!("o = {:?}", c.get("o").await?);
        println!("o.len = {:?}", c.object("o").len().await?);
        println!("o = {:?}", c.object("o").pop_back().await?);
        println!("o = {:?}", c.object("o").pop_front().await?);
        c.object("o").push_back(4).await?;
        c.object("o").push_front(3).await?;
        println!("o = {:?}", c.object("0").get(1).await?);
        println!("o = {:?}", c.get("o").await?);
    }

    {
        let c = db.collection::<List<Int64>>("ListTxn<Int64>");
        println!("collection {}", c.name());
        let mut txn = c.object("txn").begin();
        txn.push_back(1).push_front(2).set(1, 3);
        txn.commit().await?;
        println!("txn = {:?}", c.get("txn").await?);
    }

    Ok(())
}
