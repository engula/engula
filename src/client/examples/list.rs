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
use engula_client::{Any, Blob, Collection, Int64, List, Universe};

#[tokio::main]
async fn main() -> Result<()> {
    let url = "http://localhost:21716";
    let uv = Universe::connect(url).await?;
    let db = uv.database("db");

    {
        let c: Collection<List<Any>> = db.collection("c");
        c.set("o", ["hello".into(), "world".into()]).await?;
        println!("{:?}", c.get("o").await?);
        println!("{:?}", c.object("o").len().await?);
        println!("{:?}", c.object("o").pop().await?);
        c.object("o").push("richard").await?;
        println!("{:?}", c.object("o").get(1).await?);
        c.object("o").set(1, "world").await?;
        println!("{:?}", c.get("o").await?);
    }

    {
        let c: Collection<List<Blob>> = db.collection("c");
        c.set("o", [vec![1], vec![2]]).await?;
        println!("{:?}", c.get("o").await?);
        println!("{:?}", c.object("o").len().await?);
        println!("{:?}", c.object("o").pop().await?);
        c.object("o").push(vec![3]).await?;
        println!("{:?}", c.object("0").get(1).await?);
        println!("{:?}", c.get("o").await?);
    }

    {
        let c: Collection<List<Int64>> = db.collection("c");
        c.set("o", [1, 2]).await?;
        println!("{:?}", c.get("o").await?);
        println!("{:?}", c.object("o").len().await?);
        println!("{:?}", c.object("o").pop().await?);
        c.object("o").push(3).await?;
        println!("{:?}", c.object("0").get(1).await?);
        println!("{:?}", c.get("o").await?);
    }

    Ok(())
}
