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

mod error;
mod object;
mod storage;
mod uploader;

pub use self::{
    error::{Error, Result},
    object::MemObject,
    storage::MemStorage,
};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::*;

    #[tokio::test]
    async fn test() -> Result<()> {
        let s = MemStorage::default();
        s.create_bucket("a").await?;

        let data = vec![0, 1, 2];
        let mut up = s.upload_object("a", "b").await?;
        up.write(&data).await?;
        up.finish().await?;
        let object = s.object("a", "b").await?;

        let mut buf = [0u8; 3];
        let pos = 1;
        let len = object.read_at(&mut buf, pos).await?;
        assert_eq!(len, data.len() - pos);
        assert_eq!(buf[..len], data[pos..]);
        Ok(())
    }
}
