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
    let db = uv.create_database("any").await?;
    let co = db.create_collection("any").await?;

    co.set("a", 1).await?;
    let a: i64 = co.get("a").await?;
    println!("a = {:?}", a);
    co.delete("a").await?;
    let a: Option<i64> = co.get("a").await?;
    println!("delete(a) = {:?}", a);

    co.set("a", [1.0, 2.0]).await?;
    let a: Vec<f64> = co.get("a").await?;
    println!("a = {:?}", a);
    co.delete("a").await?;
    let a: Option<Vec<f64>> = co.get("a").await?;
    println!("delete(a) = {:?}", a);

    Ok(())
}
