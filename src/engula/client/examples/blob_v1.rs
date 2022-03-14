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

    co.set("a", Blob::new([1, 2])).await?;
    let a: Vec<u8> = co.get("a").await?;
    println!("a = {:?}", a);

    co.mutate("a", Blob::rpush([3, 4])).await?;
    let a: Vec<u8> = co.get("a").await?;
    println!("a.rpush([3, 4]) = {:?}", a);
    let a: Vec<u8> = co.mutate("a", Blob::lpop(2)).await?;
    println!("a.lpop(2) = {:?}", a);

    let len: i64 = co.select("a", Blob::len()).await?;
    println!("a.len() = {:?}", len);
    let a: Vec<u8> = co.select("a", Blob::range(5..)).await?;
    println!("a.range(5..) = {:?}", a);

    let mut txn = co.begin();
    txn.mutate("a", Blob::lpush([1, 2]));
    txn.mutate("b", Blob::rpush([3, 4]));
    txn.commit().await?;
    println!("a = {:?}", co.get("a").await?);
    println!("b = {:?}", co.get("b").await?);

    Ok(())
}
