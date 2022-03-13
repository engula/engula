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
use engula_client::v1::{Text, Universe};

#[tokio::main]
async fn main() -> Result<()> {
    let url = "http://localhost:21716";
    let uv = Universe::connect(url).await?;
    let db = uv.create_database("text").await?;
    let co = db.create_collection("text").await?;

    co.set("a", Text::new("hello")).await?;
    let a: Vec<u8> = co.get("a").await?;
    println!("a = {:?}", a);

    co.mutate("a", Text::rpush("world")).await?;
    let a: Vec<u8> = co.get("a").await?;
    println!("a.rpush(\"world\") = {:?}", a);

    let a: Vec<u8> = co.mutate("a", Text::lpop(5)).await?;
    println!("a.lpop(5) = {:?}", a);

    let len: i64 = co.select("a", Text::len()).await?;
    println!("a.len() = {:?}", len);
    let a: Vec<u8> = co.select("a", Text::range(5..)).await?;
    println!("a.range(5..) = {:?}", a);

    let mut txn = co.begin();
    txn.mutate("a", Text::lpush("hello"));
    txn.mutate("b", Text::rpush("world"));
    txn.commit().await?;
    println!("a = {:?}", co.get("a").await?);
    println!("b = {:?}", co.get("b").await?);

    Ok(())
}
