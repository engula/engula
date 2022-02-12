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
    let db = uv.database("db");
    let co = db.collection("co");

    co.object("o").set(1).await?;
    println!("{:?}", co.object("o").get().await?);
    co.object("o").add(2).await?;
    println!("{:?}", co.object("o").get().await?);
    co.object("o").delete().await?;
    println!("{:?}", co.object("o").get().await?);

    let mut txn = co.begin();
    txn.object("a").set(1);
    txn.object("b").add(2);
    txn.commit().await?;
    println!("a = {:?}", co.object("a").get().await?);
    println!("b = {:?}", co.object("b").get().await?);

    let mut txn = co.begin();
    txn.object("a").add(2);
    txn.object("b").delete();
    txn.commit().await?;
    println!("a = {:?}", co.object("a").get().await?);
    println!("b = {:?}", co.object("b").get().await?);

    Ok(())
}
