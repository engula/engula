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
use engula_engine::objects::string::RawString;
use tracing::debug;

use crate::{Db, Frame, Parse};

/// Get the value of key.
///
/// If the key does not exist the special value nil is returned. An error is
/// returned if the value stored at key is not a string, because GET only
/// handles string values.
#[derive(Debug)]
pub struct Get {
    /// Name of the key to get
    key: Bytes,
}

impl Get {
    /// Create a new `Get` command which fetches `key`.
    pub fn new(key: Bytes) -> Get {
        Get { key }
    }

    /// Get the key
    pub fn key(&self) -> &[u8] {
        &self.key
    }

    /// Parse a `Get` instance from a received frame.
    ///
    /// The `Parse` argument provides a cursor-like API to read fields from the
    /// `Frame`. At this point, the entire frame has already been received from
    /// the socket.
    ///
    /// The `GET` string has already been consumed.
    ///
    /// # Returns
    ///
    /// Returns the `Get` value on success. If the frame is malformed, `Err` is
    /// returned.
    ///
    /// # Format
    ///
    /// Expects an array frame containing two entries.
    ///
    /// ```text
    /// GET key
    /// ```
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Get> {
        // The `GET` string has already been consumed. The next value is the
        // name of the key to get. If the next value is not a string or the
        // input is fully consumed, then an error is returned.
        let key = parse.next_bytes()?;

        match parse.finish() {
            Ok(()) => Ok(Get { key }),
            Err(_) => Err("wrong number of arguments for 'get' command".into()),
        }
    }

    /// Apply the `Get` command to the specified `Db` instance.
    ///
    /// The response is written to `dst`. This is called by the server in order
    /// to execute a received command.
    pub(crate) fn apply(self, db: &Db) -> crate::Result<Frame> {
        // Get the value from the shared database state
        let response = if let Some(object_ref) = db.get(&self.key) {
            if let Some(value) = object_ref.data::<RawString>() {
                // If a value is present, it is written to the client in "bulk"
                // format.
                Frame::Bulk(value.data_slice().to_vec().into())
            } else {
                Frame::Error("Operation against a key holding the wrong kind of value".into())
            }
        } else {
            // If there is no value, `Null` is written.
            Frame::Null
        };

        debug!(?response);

        Ok(response)
    }

    /// Converts the command into an equivalent `Frame`.
    ///
    /// This is called by the client when encoding a `Get` command to send to
    /// the server.
    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("get".as_bytes()));
        frame.push_bulk(self.key);
        frame
    }
}
