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

mod manifest;
pub mod proto;
mod version;
mod version_set;

pub use self::{
    proto::{
        version_edit::{Bucket, File},
        VersionEditBuilder,
    },
    version::{BucketVersion, FileMetadata, OrdByUpperBound, Version},
    version_set::VersionSet,
};

#[cfg(test)]
mod test_version_set {
    use super::*;
    use crate::{versions::proto::*, *};

    #[tokio::test]
    async fn test_new_create() -> Result<()> {
        let tmp = tempdir::TempDir::new("test3")?.into_path();
        print!("{:?}", &tmp);
        let vs1 = VersionSet::open(&tmp).await?;

        for i in 1..200 {
            vs1.log_and_apply(
                VersionEditBuilder::default()
                    .add_buckets(vec![version_edit::Bucket {
                        name: format!("b{}", i).to_string(),
                    }])
                    .build(),
            )
            .await?;
        }

        let vs2 = VersionSet::open(&tmp).await?;
        for i in 200..400 {
            vs2.log_and_apply(
                VersionEditBuilder::default()
                    .add_buckets(vec![version_edit::Bucket {
                        name: format!("b{}", i).to_string(),
                    }])
                    .build(),
            )
            .await?;
        }

        Ok(())
    }
}
