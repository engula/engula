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
use engula_client::{Any, Blob, List, Universe, I64};

#[tokio::main]
async fn main() -> Result<()> {
    let url = "http://localhost:21716";
    let uv = Universe::connect(url).await?;
    let db = uv.create_database("list").await?;

    {
        let c = db.create_collection::<List<Any>>("list<any>").await?;
        println!("{}", c.name());
        let mut txn = c.object("o").begin();
        txn.store(vec![1.into(), 2.into()])
            .push_back("3")
            .push_front("0");
        txn.commit().await?;
        println!("o = {:?}", c.object("o").load().await?);
        println!("o.len = {:?}", c.object("o").len().await?);
    }

    {
        let c = db.create_collection::<List<I64>>("list<i64>").await?;
        println!("{}", c.name());
        c.set("o", [1, 2]).await?;
        println!("o = {:?}", c.get("o").await?);
        c.object("o").push_back(3).await?;
        c.object("o").push_front(0).await?;
        println!("o = {:?}", c.object("o").load().await?);
        println!("o.len = {:?}", c.object("o").len().await?);
    }

    {
        let c = db.create_collection::<List<Blob>>("list<blob>").await?;
        println!("{}", c.name());
        c.set("o", [vec![1, 2], vec![3, 4]]).await?;
        println!("o = {:?}", c.get("o").await?);
        c.object("o").push_back(vec![5, 6]).await?;
        c.object("o").push_front(vec![0]).await?;
        println!("o = {:?}", c.object("o").load().await?);
        println!("o.len = {:?}", c.object("o").len().await?);
    }

    Ok(())
}
