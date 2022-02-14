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
use engula_client::{Collection, Int64, List, Object, Universe};

#[tokio::main]
async fn main() -> Result<()> {
    let url = "http://localhost:21716";
    let uv = Universe::connect(url).await?;
    let db = uv.database("db");
    let co: Collection<List<Object>> = db.collection("co");
    let c1: Collection<List<Int64>> = db.collection("c1");

    co.set("o", vec!["o".into()]).await?;
    println!("{:?}", co.get("o").await?);
    co.delete("o").await?;
    println!("{:?}", co.get("o").await?);

    c1.set("o", vec![1]).await?;
    println!("{:?}", c1.get("o").await?);
    c1.delete("o").await?;
    println!("{:?}", c1.get("o").await?);

    Ok(())
}
