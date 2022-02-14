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
use engula_client::{Collection, Object, Universe};

#[tokio::main]
async fn main() -> Result<()> {
    let url = "http://localhost:21716";
    let uv = Universe::connect(url).await?;
    let db = uv.database("db");
    let c1: Collection<Object> = db.collection("c1");
    let c2: Collection<Object> = db.collection("c2");

    let txn = db.begin();
    {
        let mut t = txn.collection("c1");
        t.set("a1", 1);
        t.set("a2", 2);
        t.commit().await?;
    }
    {
        let mut t = txn.collection("c2");
        t.set("b1", "b1");
        t.set("b2", "b2");
        t.commit().await?;
    }
    txn.commit().await?;

    println!("a1 = {:?}", c1.get("a1").await?);
    println!("a2 = {:?}", c1.get("a2").await?);
    println!("b1 = {:?}", c2.get("b1").await?);
    println!("b2 = {:?}", c2.get("b2").await?);

    Ok(())
}
