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

//! A [`Storage`] implementation that stores data in memory.
//!
//! [`Storage`]: crate::Storage

mod storage;

pub use self::storage::Storage;

#[cfg(test)]
mod tests {
    use engula_runtime::io::{RandomReadExt, SequentialWriteExt};

    use crate::*;

    #[tokio::test]
    async fn test() -> Result<()> {
        let bucket_name = "bucket";
        let object_name = "object";

        let s = super::Storage::default();
        s.create_bucket(bucket_name).await?;

        let data = vec![0, 1, 2];
        let mut writer = s.new_sequential_writer(bucket_name, object_name).await?;
        writer.write_all(&data).await?;
        writer.close().await?;

        let mut reader = s.new_random_reader(bucket_name, object_name).await?;
        let mut buf = vec![0; 2];
        let pos = 1;
        reader.read_exact(&mut buf, pos).await?;
        assert_eq!(buf, data[pos..(pos + buf.len())]);

        Ok(())
    }
}
