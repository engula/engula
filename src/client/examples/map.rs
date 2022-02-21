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
use engula_client::{Any, Blob, Map, Universe, I64};

#[tokio::main]
async fn main() -> Result<()> {
    let url = "http://localhost:21716";
    let uv = Universe::connect(url).await?;
    let db = uv.create_database("map").await?;

    let (k1, k2) = (vec![1], vec![2]);

    {
        let c = db.create_collection::<Map<Any>>("map<any>").await?;
        println!("{}", c.name());
        let mut txn = c.object("o").begin();
        txn.store([(k1.clone(), 1.into()), (k2.clone(), "2".into())]);
        txn.commit().await?;
        println!("o = {:?}", c.get("o").await?);
    }

    {
        let c = db.create_collection::<Map<I64>>("map<i64>").await?;
        println!("{}", c.name());
        c.set("o", [(k1.clone(), 1), (k2.clone(), 2)]).await?;
        println!("o = {:?}", c.get("o").await?);
        println!("o = {:?}", c.object("o").load().await?);
        println!("o.len = {:?}", c.object("o").len().await?);
    }

    {
        let c = db.create_collection::<Map<Blob>>("map<blob>").await?;
        println!("{}", c.name());
        c.set("o", [(k1.clone(), k1.clone()), (k2.clone(), k2.clone())])
            .await?;
        println!("o = {:?}", c.get("o").await?);
        println!("o = {:?}", c.object("o").load().await?);
        println!("o.len = {:?}", c.object("o").len().await?);
    }

    Ok(())
}
