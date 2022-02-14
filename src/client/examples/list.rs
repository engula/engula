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
use engula_client::{Blob, Collection, Int64, List, Object, Universe};

#[tokio::main]
async fn main() -> Result<()> {
    let url = "http://localhost:21716";
    let uv = Universe::connect(url).await?;
    let db = uv.database("db");
    let c0: Collection<List<Blob>> = db.collection("c0");
    let c1: Collection<List<Int64>> = db.collection("c1");
    let c2: Collection<List<Object>> = db.collection("c2");

    c0.set("o", vec![vec![1, 2], vec![3, 4]]).await?;
    println!("{:?}", c0.get("o").await?);
    println!("{:?}", c0.object("o").len().await?);
    println!("{:?}", c0.object("o").pop().await?);
    c0.object("o").push(vec![5, 6]).await?;
    c0.object("o").push(vec![7, 8]).await?;
    println!("{:?}", c0.get("o").await?);
    c0.delete("o").await?;
    println!("{:?}", c0.get("o").await?);

    c1.set("o", vec![1]).await?;
    println!("{:?}", c1.get("o").await?);
    println!("{:?}", c1.object("o").len().await?);
    println!("{:?}", c1.object("o").pop().await?);
    c1.object("o").push(1).await?;
    println!("{:?}", c1.get("o").await?);
    c1.delete("o").await?;
    println!("{:?}", c1.get("o").await?);

    c2.set("o", vec!["hello".into()]).await?;
    println!("{:?}", c2.get("o").await?);
    c2.object("o").set(0, "world").await?;
    println!("{:?}", c2.object("o").get(0).await?);
    c2.object("o").delete(0).await?;
    println!("{:?}", c2.object("o").get(0).await?);
    c2.delete("o").await?;
    println!("{:?}", c2.get("o").await?);

    Ok(())
}
