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
    let url = "http://localhost:21716";
    let uv = Universe::connect(url).await?;
    let db = uv.create_database("txn").await?;
    let c0 = db.create_collection::<Any>("any").await?;
    let c1 = db.create_collection::<I64>("i64").await?;
    let c2 = db.create_collection::<Blob>("blob").await?;
    let c3 = db.create_collection::<List<I64>>("list<i64>").await?;
    let c4 = db.create_collection::<Map<Blob>>("map<blob>").await?;

    let txn = db.begin();
    {
        println!("{}", c0.name());
        let mut t = c0.begin_with(txn.clone());
        t.object("a").add(1).sub(2);
        t.object("b").store(vec![1u8, 2u8]);
        t.commit().await?;
    }
    {
        println!("{}", c1.name());
        let mut t = c1.begin_with(txn.clone());
        t.object("a").add(1).sub(2);
        t.object("b").sub(1).add(2);
        t.commit().await?;
    }
    {
        println!("{}", c2.name());
        let mut t = c2.begin_with(txn.clone());
        t.object("a").append(vec![1, 2]).append(vec![3, 4]);
        t.object("b").append(vec![5, 6]).append(vec![7, 8]);
        t.commit().await?;
    }
    {
        println!("{}", c3.name());
        let mut t = c3.begin_with(txn.clone());
        t.object("a").push_back(1).push_back(2);
        t.object("b").push_front(1).push_front(2);
        t.commit().await?;
    }
    {
        println!("{}", c4.name());
        let c = c4.begin_with(txn.clone());
        // TODO
        c.commit().await?;
    }
    txn.commit().await?;

    {
        println!("c0[a] = {:?}", c0.get("a").await?);
        println!("c0[b] = {:?}", c0.get("b").await?);
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
