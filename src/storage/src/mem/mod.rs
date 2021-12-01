// Copyright 2021 The Engula Authors.
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

//! A storage implementation that stores data in memory.

mod bucket;
mod storage;

pub use self::{bucket::Bucket, storage::Storage};

#[cfg(test)]
mod tests {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use crate::*;

    #[tokio::test]
    async fn test() -> Result<()> {
        let s = super::Storage::default();
        let bucket = s.create_bucket("a").await?;

        let name = "abc";
        let data = vec![0, 1, 2];
        let mut writer = bucket.new_sequential_writer(name).await?;
        writer.write_all(&data).await?;
        writer.shutdown().await?;

        let mut reader = bucket.new_sequential_reader(name).await?;
        let mut got = Vec::new();
        reader.read_to_end(&mut got).await?;
        assert_eq!(got, data);

        Ok(())
    }
}
