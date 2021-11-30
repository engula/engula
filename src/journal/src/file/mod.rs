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
mod journal;
mod stream;

pub use self::{
    error::{Error, Result},
    journal::FileJournal,
    stream::FileStream,
};

#[cfg(test)]
mod tests {
    use std::{env, path::PathBuf, process};

    use super::{
        error::{Error, Result},
        *,
    };
    use crate::*;

    struct TestEnvGuard {
        path: PathBuf,
    }

    impl TestEnvGuard {
        fn setup(case: &str) -> Self {
            let mut path = env::temp_dir();
            path.push("engula-storage-fs-test");
            path.push(process::id().to_string());
            path.push(case);
            Self { path }
        }
    }

    impl Drop for TestEnvGuard {
        fn drop(&mut self) {
            std::fs::remove_dir_all(&self.path).unwrap();
        }
    }

    #[tokio::test]
    async fn test_bucket_manage() -> Result<()> {
        let g = TestEnvGuard::setup("test_local_journal");
        let s = FileStream::new(&g.path).await?;

    }

    #[tokio::test]
    async fn test() -> Result<()> {
        let j: MemJournal<u64> = MemJournal::default();
        let stream = j.create_stream("a").await?;
        let event = Event {
            ts: 0,
            data: vec![1, 2, 3],
        };
        stream.append_event(event.clone()).await?;
        let mut events = stream.read_events(0).await?;
        let got = events.next().await.unwrap()?;
        assert_eq!(got, event);
        Ok(())
    }
}
