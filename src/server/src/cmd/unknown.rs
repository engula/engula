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

use tracing::debug;

use super::*;
use crate::{async_trait, Db, Frame};

/// Represents an "unknown" command. This is not a real `Redis` command.
#[derive(Debug)]
pub struct Unknown {
    command_name: String,
}

impl Unknown {
    /// Create a new `Unknown` command which responds to unknown commands
    /// issued by clients
    pub(crate) fn new(key: impl ToString) -> Unknown {
        Unknown {
            command_name: key.to_string(),
        }
    }
}

#[async_trait(?Send)]
impl CommandAction for Unknown {
    /// Responds to the client, indicating the command is not recognized.
    ///
    /// This usually means the command is not yet implemented by `mini-redis`.
    async fn apply(&self, _: &Db) -> crate::Result<Frame> {
        let response = Frame::Error(format!("ERR unknown command '{}'", self.command_name));

        debug!(?response);

        Ok(response)
    }

    fn get_name(&self) -> &str {
        &self.command_name
    }
}
