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
use engula_client::{Any, Blob, Int64, Universe, UnorderedMap};

#[tokio::main]
async fn main() -> Result<()> {
    let url = "http://localhost:21716";
    let uv = Universe::connect(url).await?;
    let db = uv.database("db");

    let (k1, k2) = (vec![1], vec![2]);

    {
        let c = db.collection::<UnorderedMap<Any>>("c");
        c.set("o", [(k1.clone(), "a".into()), (k2.clone(), "b".into())])
            .await?;
        println!("{:?}", c.get("o").await?);
        println!("{:?}", c.object("o").len().await?);
        c.object("o").set(k1.clone(), "b").await?;
        println!("{:?}", c.object("o").get(k1.clone()).await?);
        c.object("o").remove(k1.clone()).await?;
        println!("{:?}", c.get("o").await?);
    }

    {
        let c = db.collection::<UnorderedMap<Blob>>("c");
        c.set("o", [(k1.clone(), k1.clone()), (k2.clone(), k2.clone())])
            .await?;
        println!("{:?}", c.get("o").await?);
        println!("{:?}", c.object("o").len().await?);
        c.object("o").set(k1.clone(), k2.clone()).await?;
        println!("{:?}", c.object("o").get(k1.clone()).await?);
        c.object("o").remove(k1.clone()).await?;
        println!("{:?}", c.get("o").await?);
    }

    {
        let c = db.collection::<UnorderedMap<Int64>>("c");
        c.set("o", [(k1.clone(), 1), (k2.clone(), 2)]).await?;
        println!("{:?}", c.get("o").await?);
        println!("{:?}", c.object("o").len().await?);
        c.object("o").set(k1.clone(), 2).await?;
        println!("{:?}", c.object("o").get(k1.clone()).await?);
        c.object("o").remove(k1.clone()).await?;
        println!("{:?}", c.get("o").await?);
    }

    Ok(())
}
