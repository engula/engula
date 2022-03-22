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
use engula_client::Blob;

use crate::create_universe;

#[tokio::test]
#[ignore]
async fn test_apis() -> Result<()> {
    let uv = create_universe().await?;
    let db = uv.create_database("blob").await?;
    let co = db.create_collection("blob").await?;

    co.set("o", Blob::value([1, 2])).await?;
    let o: Vec<u8> = co.get("o").await?;
    assert_eq!(vec![1, 2], o);

    co.mutate("o", Blob::rpush([3, 4])).await?;
    let o: Vec<u8> = co.get("o").await?;
    assert_eq!(vec![1, 2, 3, 4], o);
    let len: i64 = co.select("o", Blob::len()).await?;
    assert_eq!(4, len);

    Ok(())
}
