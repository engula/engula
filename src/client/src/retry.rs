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

use tokio::time;

use crate::Error;

pub struct RetryState {
    cnt: u64,
    interval: Duration,
}

impl RetryState {
    pub fn new(cnt: u64, interval: Duration) -> Self {
        Self { cnt, interval }
    }

    pub async fn retry(&mut self, err: &Error) -> bool {
        if self.cnt > 0 && err.should_retry() {
            self.cnt -= 1;
            time::sleep(self.interval).await;
            true
        } else {
            false
        }
    }
}
