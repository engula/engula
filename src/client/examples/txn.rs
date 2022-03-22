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

use std::collections::HashMap;

use anyhow::Result;
use engula_client::{Blob, List, Map, Universe, I64};

#[tokio::main]
async fn main() -> Result<()> {
    let url = "http://localhost:21716";
    let uv = Universe::connect(url).await?;
    let db = uv.create_database("db").await?;
    let co1 = db.create_collection("co1").await?;
    let co2 = db.create_collection("co2").await?;

    let txn = db.begin();
    {
        let mut t = txn.collection("co1");
        t.set("a", 1);
        t.set("b", Blob::value([1, 2]));
        t.submit();
    }
    {
        let mut t = txn.collection("co2");
        t.set("c", List::value([3, 4]));
        t.set("d", Map::value([(5, 5), (6, 6)]));
        t.submit();
    }
    txn.commit().await?;

    let mut tx1 = co1.begin();
    tx1.mutate("a", I64::add(1));
    tx1.mutate("b", Blob::rpush([3, 4]));
    tx1.commit().await?;

    let mut tx2 = co2.begin();
    tx2.mutate("c", List::lpush([1, 2]));
    tx2.mutate("d", Map::extend([(3, 3), (4, 4)]));
    tx2.commit().await?;

    let a: i64 = co1.get("a").await?;
    let b: Vec<u8> = co1.get("b").await?;
    let c: Vec<i64> = co2.get("c").await?;
    let d: HashMap<i64, i64> = co2.get("d").await?;
    println!("a = {:?}", a);
    println!("b = {:?}", b);
    println!("c = {:?}", c);
    println!("d = {:?}", d);

    Ok(())
}
