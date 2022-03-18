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

use std::collections::HashSet;

use anyhow::Result;
use engula_client::{Set, Universe};

#[tokio::main]
async fn main() -> Result<()> {
    let url = "http://localhost:21716";
    let uv = Universe::connect(url).await?;
    let db = uv.create_database("map").await?;
    let co = db.create_collection("map").await?;

    let va = [0, 1, 2];
    let vb = [3, 4, 5];

    co.set("a", Set::new(va)).await?;
    let a: HashSet<i64> = co.get("a").await?;
    println!("a = {:?}", a);

    co.mutate("a", Set::extend(vb)).await?;
    let a: HashSet<i64> = co.get("a").await?;
    println!("a.extend({:?}) = {:?}", vb, a);

    co.mutate("a", Set::remove([0, 1])).await?;
    let a: HashSet<i64> = co.get("a").await?;
    println!("a.remove([0, 1]) = {:?}", a);

    let len: i64 = co.select("a", Set::len()).await?;
    println!("a.len() = {:?}", len);
    let a: HashSet<i64> = co.select("a", Set::range(2..)).await?;
    println!("a.range(2..) = {:?}", a);
    let a: bool = co.select("a", Set::contains([1, 2])).await?;
    println!("a.contains([1, 2]) = {:?}", a);

    Ok(())
}
