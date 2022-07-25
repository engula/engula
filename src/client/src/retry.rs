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

use tracing::trace;

use crate::{Error, Result};

pub struct RetryState {
    cnt: u64,
    interval: Duration,
}

impl Default for RetryState {
    fn default() -> Self {
        RetryState::new(10, Duration::from_millis(200))
    }
}

impl RetryState {
    pub fn new(cnt: u64, interval: Duration) -> Self {
        Self { cnt, interval }
    }

    pub async fn retry(&mut self, err: Error) -> Result<()> {
        match err {
            Error::NotFound(_) | Error::EpochNotMatch(_) => {
                self.cnt -= 1;
                trace!("retry state cnt {}", self.cnt);
                if self.cnt == 0 {
                    return Err(Error::DeadlineExceeded("timeout".into()));
                }
                tokio::time::sleep(self.interval).await;
                Ok(())
            }
            Error::NotLeader(..) | Error::GroupNotFound(_) | Error::NotRootLeader(..) => {
                unreachable!()
            }
            Error::InvalidArgument(_)
            | Error::DeadlineExceeded(_)
            | Error::ResourceExhausted(_)
            | Error::AlreadyExists(_)
            | Error::Rpc(_)
            | Error::Internal(_) => Err(err),
        }
    }
}
