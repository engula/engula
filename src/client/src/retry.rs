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

use std::time::{Duration, Instant};

use crate::{Error, Result};

pub struct RetryState {
    interval_ms: u64,
    deadline: Option<Instant>,
}

impl Default for RetryState {
    fn default() -> Self {
        RetryState::new(None)
    }
}

impl RetryState {
    pub fn new(timeout: Option<Duration>) -> Self {
        RetryState {
            interval_ms: 8,
            deadline: timeout.and_then(|d| Instant::now().checked_add(d)),
        }
    }

    #[inline]
    pub fn timeout(&self) -> Option<Duration> {
        self.deadline
            .map(|d| d.saturating_duration_since(Instant::now()))
    }

    pub async fn retry(&mut self, err: Error) -> Result<()> {
        match err {
            Error::NotFound(_) | Error::EpochNotMatch(_) | Error::GroupNotAccessable(_) => {
                let mut interval = Duration::from_millis(self.interval_ms);
                if let Some(deadline) = self.deadline {
                    if let Some(duration) = deadline.checked_duration_since(Instant::now()) {
                        interval = std::cmp::min(interval, duration);
                    } else {
                        return Err(Error::DeadlineExceeded("timeout".into()));
                    }
                }
                tokio::time::sleep(interval).await;
                self.interval_ms = std::cmp::min(self.interval_ms * 2, 250);
                Ok(())
            }
            Error::NotLeader(..)
            | Error::GroupNotFound(_)
            | Error::NotRootLeader(..)
            | Error::Connect(_) => {
                unreachable!()
            }
            Error::InvalidArgument(_)
            | Error::DeadlineExceeded(_)
            | Error::ResourceExhausted(_)
            | Error::AlreadyExists(_)
            | Error::Rpc(_)
            | Error::Transport(_)
            | Error::Internal(_) => Err(err),
        }
    }
}
