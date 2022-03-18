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

    let db = uv.create_database("db").await?;
    println!("created {:?}", db.desc().await?);
    let db2 = uv.create_database("db2").await?;
    println!("created {:?}", db2.desc().await?);

    let co = db.create_collection("co").await?;
    println!("created {:?}", co.desc().await?);
    let co2 = db.create_collection("co2").await?;
    println!("created {:?}", co2.desc().await?);

    println!("list databases {:?}", uv.list_databases().collect().await?);
    println!(
        "list collections {:?}",
        db.list_collections().collect().await?
    );

    uv.delete_database(db2.name()).await?;
    db.delete_collection(co2.name()).await?;

    Ok(())
}
