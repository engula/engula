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

mod block_builder;
mod block_handle;
mod block_iter;
mod block_reader;
mod table_builder;
mod table_iter;
mod table_reader;

pub use self::table_builder::{TableBuilder, TableBuilderOptions};

#[cfg(test)]
mod tests {
    use tokio::fs::File;

    use super::*;
    use crate::Result;

    #[tokio::test]
    async fn table() -> Result<()> {
        let file = tempfile::tempfile()?;
        let options = TableBuilderOptions::default();
        let mut builder = TableBuilder::new(options, File::from_std(file));
        for i in 0..1024u64 {
            let k = i.to_be_bytes();
            builder.add(&k, &k).await?;
        }
        builder.finish().await?;
        Ok(())
    }
}
