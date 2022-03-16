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
use engula_client::v1::{Blob, Universe};

#[tokio::main]
async fn main() -> Result<()> {
    let url = "http://localhost:21716";
    let uv = Universe::connect(url).await?;
    let db = uv.create_database("blob").await?;
    let co = db.create_collection("blob").await?;

    co.set("a", Blob::new([1, 2, 3, 4])).await?;
    let a: Vec<u8> = co.get("a").await?;
    println!("a = {:?}", a);

    co.mutate("a", Blob::rpush([5, 6, 7, 8])).await?;
    let a: Vec<u8> = co.get("a").await?;
    println!("a.rpush([5, 6, 7, 8]) = {:?}", a);
    let a: Vec<u8> = co.mutate("a", Blob::rpop(2)).await?;
    println!("a.rpop(2) = {:?}", a);
    let a: Vec<u8> = co.mutate("a", Blob::lpop(2)).await?;
    println!("a.lpop(2) = {:?}", a);
    co.mutate("a", Blob::lpush([1, 2])).await?;
    let a: Vec<u8> = co.get("a").await?;
    println!("a.lpush([1, 2]) = {:?}", a);
    co.mutate("a", Blob::trim(2..)).await?;
    let a: Vec<u8> = co.get("a").await?;
    println!("a.trim(2..) = {:?}", a);

    let a: i64 = co.select("a", Blob::len()).await?;
    println!("a.len() = {:?}", a);
    let a: Vec<u8> = co.select("a", Blob::range(1..)).await?;
    println!("a.range(1..) = {:?}", a);
    let a: Vec<u8> = co.select("a", Blob::range(..-1)).await?;
    println!("a.range(..-1) = {:?}", a);

    let mut txn = co.begin();
    txn.mutate("a", Blob::lpush([1, 2]));
    txn.mutate("b", Blob::rpush([3, 4]));
    txn.commit().await?;
    let a: Vec<u8> = co.get("a").await?;
    let b: Vec<u8> = co.get("b").await?;
    println!("a.lpush([1, 2]) = {:?}", a);
    println!("b.rpush([3, 4]) = {:?}", b);

    Ok(())
}
