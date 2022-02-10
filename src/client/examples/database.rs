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
use engula_client::Universe;

#[tokio::main]
async fn main() -> Result<()> {
    let url = "http://localhost:21716";
    let uv = Universe::connect(url).await?;
    let db = uv.database("db").await?;
    let c1 = db.collection("c1").await?;
    let c2 = db.collection("c2").await?;

    let txn = db.begin();
    {
        let mut tx1 = c1.begin_with(txn.clone());
        tx1.object("a1").set(1);
        tx1.object("a2").add(2);
        tx1.commit().await?;
    }
    {
        let mut tx2 = c2.begin_with(txn.clone());
        tx2.object("b1").set(3);
        tx2.object("b2").add(4);
        tx2.commit().await?;
    }
    txn.commit().await?;

    println!("a1 = {:?}", c1.object("a1").get().await?);
    println!("a2 = {:?}", c1.object("a2").get().await?);
    println!("b1 = {:?}", c2.object("b1").get().await?);
    println!("b2 = {:?}", c2.object("b2").get().await?);

    Ok(())
}
