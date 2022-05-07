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

use bytes::Bytes;

use super::*;
use crate::{async_trait, Db, Frame, Parse, ParseError};

/// Returns PONG if no argument is provided, otherwise
/// return a copy of the argument as a bulk.
///
/// This command is often used to test if a connection
/// is still alive, or to measure latency.
#[derive(Debug, Default)]
pub struct Ping {
    /// optional message to be returned
    msg: Option<String>,
}

impl Ping {
    /// Create a new `Ping` command with optional `msg`.
    pub fn new(msg: Option<String>) -> Ping {
        Ping { msg }
    }
}

/// Parse a `Ping` instance from a received frame.
///
/// The `Parse` argument provides a cursor-like API to read fields from the
/// `Frame`. At this point, the entire frame has already been received from
/// the socket.
///
/// The `PING` string has already been consumed.
///
/// # Returns
///
/// Returns the `Ping` value on success. If the frame is malformed, `Err` is
/// returned.
///
/// # Format
///
/// Expects an array frame containing `PING` and an optional message.
///
/// ```text
/// PING [message]
/// ```
pub(crate) fn parse_frames(
    _: &CommandDescs,
    parse: &mut Parse,
) -> crate::Result<Box<dyn CommandAction>> {
    let cmd = match parse.next_string() {
        Ok(msg) => Ping::new(Some(msg)),
        Err(ParseError::EndOfStream) => Ping::default(),
        Err(e) => return Err(e.into()),
    };

    match parse.finish() {
        Ok(()) => Ok(Box::new(cmd)),
        Err(_) => Err("wrong number of arguments for 'ping' command".into()),
    }
}

#[async_trait(?Send)]
impl CommandAction for Ping {
    /// Apply the `Ping` command and return the message.
    ///
    /// The response is written to `dst`. This is called by the server in order
    /// to execute a received command.
    async fn apply(&self, _: &Db) -> crate::Result<Frame> {
        let response = match &self.msg {
            None => Frame::Simple("PONG".to_string()),
            Some(msg) => Frame::Bulk(Bytes::from(msg.to_owned())),
        };

        Ok(response)
    }

    fn get_name(&self) -> &str {
        "ping"
    }
}
