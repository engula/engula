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
use engula_client::v1::Universe;

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
    let a: bool = co.exists("a").await?;
    println!("exists(a) = {:?}", a);

    co.set("b", "hello").await?;
    let b: String = co.get("b").await?;
    println!("b = {:?}", b);
    co.delete("b").await?;
    let b: Option<String> = co.get("b").await?;
    println!("delete(b) = {:?}", b);
    let b: bool = co.exists("b").await?;
    println!("exists(b) = {:?}", b);

    Ok(())
}
