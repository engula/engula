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

use std::time::Duration;

use engula_client::{EngulaClient, Error};
use tokio::time;

#[tokio::main]
async fn main() -> Result<(), Error> {
    tracing_subscriber::fmt::init();

    let addr = "127.0.0.1:21805";
    let client = EngulaClient::connect(addr.to_string()).await?;
    let db = client.create_database("test_db".to_string()).await?;
    let co = db.create_collection("test_co".to_string()).await?;

    // wait for watch events populated
    time::sleep(Duration::from_secs(1)).await;

    let k = "book_name".as_bytes().to_vec();
    let v = "rust_in_actions".as_bytes().to_vec();
    co.put(k.clone(), v).await?;
    let r = co.get(k).await?;
    println!("{:?}", r);
    Ok(())
}
