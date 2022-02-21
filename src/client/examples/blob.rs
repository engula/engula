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
use engula_client::{Blob, Universe};

#[tokio::main]
async fn main() -> Result<()> {
    let url = "http://localhost:21716";
    let uv = Universe::connect(url).await?;
    let db = uv.create_database("blob").await?;
    let co = db.create_collection::<Blob>("blob").await?;

    co.set("o", vec![1, 2]).await?;
    println!("o = {:?}", co.get("o").await?);
    co.object("o").append(vec![3, 4]).await?;
    println!("o = {:?}", co.get("o").await?);
    println!("o.len = {:?}", co.object("o").len().await?);

    let mut txn = co.object("o").begin();
    txn.append(vec![5, 6]).append(vec![7, 8]);
    txn.commit().await?;
    println!("o = {:?}", co.object("o").load().await?);

    Ok(())
}
