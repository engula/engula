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

use bitflags::bitflags;
use bytes::{BufMut, Bytes, BytesMut};
use engula_engine::{
    elements::{array::Array, BoxElement},
    objects::{string::RawString, BoxObject},
    time::unix_timestamp_millis,
};
use tracing::debug;

use super::*;
use crate::{async_trait, parse::Parse, Db, Frame};

bitflags! {
    pub struct Flags: u32 {
        const NONE = 0;
        /// Only set the key if it does not already exists
        const SET_NX = 0b0001;
        /// Only set the key if it already exists
        const SET_XX = 0b0010;
        /// Whether return the old string stored at key
        const SET_GET = 0b0100;
        /// Retain the time to live associated with the key
        const SET_KEEP_TTL = 0b1000;
        /// With the specified expire time
        const SET_EXPIRE = 0b10000;
    }
}

/// Set `key` to hold the string `value`.
///
/// If `key` already holds a value, it is overwritten, regardless of its type.
/// Any previous time to live associated with the key is discarded on successful
/// SET operation.
///
/// # Options
///
/// Currently, the following options are supported:
///
/// * EX `seconds` -- Set the specified expire time, in seconds.
/// * PX `milliseconds` -- Set the specified expire time, in milliseconds.
#[derive(Debug)]
pub struct Set {
    /// the lookup key
    key: Bytes,

    /// the value to be stored
    value: Bytes,

    /// When to expire the key
    expire: u64,

    flags: Flags,
}

/// Parse a `Set` instance from a received frame.
///
/// The `Parse` argument provides a cursor-like API to read fields from the
/// `Frame`. At this point, the entire frame has already been received from
/// the socket.
///
/// The `SET` string has already been consumed.
///
/// # Returns
///
/// Returns the `Set` value on success. If the frame is malformed, `Err` is
/// returned.
///
/// # Format
///
/// Expects an array frame containing at least 3 entries.
///
/// ```text
/// SET key value [EX seconds|PX milliseconds]
/// ```
pub(crate) fn parse_frames(
    _: &CommandDescs,
    parse: &mut Parse,
) -> crate::Result<Box<dyn CommandAction>> {
    use crate::ParseError::EndOfStream;

    // Read the key to set. This is a required field
    let key = parse.next_bytes()?;

    // Read the value to set. This is a required field.
    let value = parse.next_bytes()?;

    let mut expire = 0;
    let mut flags = Flags::NONE;

    loop {
        match parse.next_string() {
            Ok(s) => match s.to_uppercase().as_str() {
                "NX" if !flags.intersects(Flags::SET_GET | Flags::SET_XX) => {
                    flags |= Flags::SET_NX;
                }
                "XX" if !flags.intersects(Flags::SET_NX) => {
                    flags |= Flags::SET_XX;
                }
                "GET" if !flags.intersects(Flags::SET_NX) => {
                    flags |= Flags::SET_GET;
                }
                "KEEPTTL" if !flags.intersects(Flags::SET_EXPIRE) => {
                    flags |= Flags::SET_KEEP_TTL;
                }
                "EX" if !flags.intersects(Flags::SET_EXPIRE | Flags::SET_KEEP_TTL) => {
                    // An expiration is specified in seconds. The next value is an
                    // integer.
                    let secs = parse.next_int()?;
                    expire = unix_timestamp_millis() + secs * 1000;
                    flags |= Flags::SET_EXPIRE;
                }
                "PX" if !flags.intersects(Flags::SET_EXPIRE | Flags::SET_KEEP_TTL) => {
                    // An expiration is specified in milliseconds. The next value is
                    // an integer.
                    let ms = parse.next_int()?;
                    expire = unix_timestamp_millis() + ms;
                    flags |= Flags::SET_EXPIRE;
                }
                "EXAT" if !flags.intersects(Flags::SET_EXPIRE | Flags::SET_KEEP_TTL) => {
                    // An expiration is specified in seconds. The next value is an
                    // integer.
                    let secs = parse.next_int()?;
                    expire = secs * 1000;
                    flags |= Flags::SET_EXPIRE;
                }
                "PXAT" if !flags.intersects(Flags::SET_EXPIRE | Flags::SET_KEEP_TTL) => {
                    // An expiration is specified in milliseconds. The next value is
                    // an integer.
                    expire = parse.next_int()?;
                    flags |= Flags::SET_EXPIRE;
                }
                _ => {
                    return Err("syntax error".into());
                }
            },
            // The `EndOfStream` error indicates there is no further data to
            // parse. In this case, it is a normal run time situation and
            // indicates there are no specified `SET` options.
            Err(EndOfStream) => break,
            // All other errors are bubbled up, resulting in the connection
            // being terminated.
            Err(err) => return Err(err.into()),
        }
    }

    Ok(Box::new(Set {
        key,
        value,
        expire,
        flags,
    }))
}

impl Set {
    /// Create a new `Set` command which sets `key` to `value`.
    pub fn new(key: Bytes, value: Bytes) -> Set {
        Set {
            key,
            value,
            expire: 0,
            flags: Flags::NONE,
        }
    }
}

#[async_trait(?Send)]
impl CommandAction for Set {
    /// Apply the `Set` command to the specified `Db` instance.
    ///
    /// The response is written to `dst`. This is called by the server in order
    /// to execute a received command.
    async fn apply(&self, db: &Db) -> crate::Result<Frame> {
        // Set the value in the shared database state.
        let value = BoxElement::<Array>::from_slice(self.value.as_ref());
        let mut object = BoxObject::<RawString>::with_key_value(self.key.as_ref(), value);
        if self.flags.contains(Flags::SET_EXPIRE) {
            object.meta.set_deadline(self.expire);
        }

        let response = match db.get(self.key.as_ref()).await {
            Some(raw_object) if !self.flags.contains(Flags::SET_NX) => {
                if self.flags.contains(Flags::SET_KEEP_TTL) {
                    object
                        .meta
                        .set_deadline(raw_object.object_meta().deadline());
                }
                if self.flags.contains(Flags::SET_GET) {
                    match raw_object.data::<RawString>() {
                        None => Frame::Error(
                            "WRONGTYPE Operation against a key holding the wrong kind of value"
                                .into(),
                        ),
                        Some(old_string) => {
                            let data = old_string.data_slice();
                            let mut bytes = BytesMut::with_capacity(data.len());
                            bytes.put_slice(data);
                            db.insert(object).await;
                            Frame::Bulk(bytes.into())
                        }
                    }
                } else {
                    db.insert(object).await;
                    Frame::Simple("OK".into())
                }
            }
            None if !self.flags.contains(Flags::SET_XX) => {
                db.insert(object).await;
                if self.flags.contains(Flags::SET_GET) {
                    Frame::Null
                } else {
                    Frame::Simple("OK".into())
                }
            }
            _ => Frame::Null,
        };

        debug!(?response);

        Ok(response)
    }

    fn get_name(&self) -> &str {
        "set"
    }
}
