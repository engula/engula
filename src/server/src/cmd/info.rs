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
use engula_engine::Db;
use tracing::debug;

use crate::{frame::Frame, parse::Parse};

// The INFO command returns information and statistics about the server in
// a format that is simple to parse by computers and easy to read by humans.
#[derive(Debug, Default)]
pub struct Info {}

impl Info {
    pub fn new() -> Self {
        Info {}
    }

    /// Parse a `Info` instance from a received frame.
    ///
    /// The `Parse` argument provides a cursor-like API to read fields from the
    /// `Frame`. At this point, the entire frame has already been received from
    /// the socket.
    ///
    /// The `INFO` string has already been consumed.
    ///
    /// # Returns
    ///
    /// Returns the `INFO` value on success. If the frame is malformed, `Err` is
    /// returned.
    ///
    /// # Format
    ///
    /// Expects an array frame containing `INFO` and some optional messages.
    ///
    /// ```text
    /// INFO [section [section...]]
    /// ``
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Info> {
        // TODO(walter) support 'all', 'default', 'everthing'.
        match parse.finish() {
            Ok(()) => Ok(Info {}),
            Err(_) => Err("wrong number of arguments for 'info' command".into()),
        }
    }

    /// Apply the `INFO` command and return the message.
    pub(crate) fn apply(self, db: &Db) -> crate::Result<Frame> {
        let db_stats = db.stats();
        let content = format!(
            r#"# Stats
evicted_keys:{evicted_keys}
keyspace_hits:{keyspace_hits}
keyspace_misses:{keyspace_misses}
# Keyspace
keys:{num_keys}
"#,
            evicted_keys = db_stats.evicted_keys,
            keyspace_hits = db_stats.keyspace_hits,
            keyspace_misses = db_stats.keyspace_misses,
            num_keys = db_stats.num_keys,
        );

        let response = Frame::Bulk(content.into());
        debug!(?response);
        Ok(response)
    }
}
