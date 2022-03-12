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
use engula_client::v1::{List, Universe};

#[tokio::main]
async fn main() -> Result<()> {
    let url = "http://localhost:21716";
    let uv = Universe::connect(url).await?;
    let db = uv.create_database("list").await?;
    let co = db.create_collection("list").await?;

    co.mutate("a", List::set(vec![1, 2])).await?;
    co.mutate("a", List::push_back(vec![3, 4])).await?;
    let a: Vec<i64> = co.object("a").await?;
    println!("a = {:?}", a);
    let len: i64 = co.select("a", List::len()).await?;
    println!("a.len = {:?}", len);
    let a: Vec<i64> = co.select("a", List::range(2..)).await?;
    println!("a.range(2..) = {:?}", a);
    let a: Vec<i64> = co.mutate("a", List::pop_back(2)).await?;
    println!("a.pop_back(2) = {:?}", a);
    let a: i64 = co.select("a", List::element(-1)).await?;
    println!("a[-1] = {:?}", a);
    let a: Vec<i64> = co.select("a", List::elements([0, 1])).await?;
    println!("a[0,1] = {:?}", a);

    let mut txn = co.begin();
    txn.mutate("a", List::push_back(vec![1, 2]));
    txn.mutate("b", List::push_front(vec![3, 4]));
    txn.commit().await?;
    println!("a = {:?}", co.object("a").await?);
    println!("b = {:?}", co.object("b").await?);

    Ok(())
}
