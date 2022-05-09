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
use engula_engine::{objects::string::RawString, time::unix_timestamp_millis};
use tracing::debug;

use super::*;
use crate::{async_trait, Db, Frame, Parse};

#[derive(Debug)]
enum GetOpt {
    None,
    Deadline(u64),
    Persist,
    Range { start: i64, end: i64 },
    Delete,
}

/// Get the value of key.
///
/// If the key does not exist the special value nil is returned. An error is
/// returned if the value stored at key is not a string, because GET only
/// handles string values.
#[derive(Debug)]
pub struct Get {
    /// Name of the key to get
    key: Bytes,

    /// option of get command
    option: GetOpt,
}

impl Get {
    /// Create a new `Get` command which fetches `key`.
    pub fn new(key: Bytes) -> Get {
        Get {
            key,
            option: GetOpt::None,
        }
    }
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
pub(crate) fn parse_frames(
    _: &CommandDescs,
    parse: &mut Parse,
) -> crate::Result<Box<dyn CommandAction>> {
    // The `GET` string has already been consumed. The next value is the
    // name of the key to get. If the next value is not a string or the
    // input is fully consumed, then an error is returned.
    let key = parse.next_bytes()?;

    Ok(Box::new(Get {
        key,
        option: GetOpt::None,
    }))
}

pub(crate) fn parse_del_frames(
    _: &CommandDescs,
    parse: &mut Parse,
) -> crate::Result<Box<dyn CommandAction>> {
    let key = parse.next_bytes()?;
    Ok(Box::new(Get {
        key,
        option: GetOpt::Delete,
    }))
}

pub(crate) fn parse_ext_frames(
    _: &CommandDescs,
    parse: &mut Parse,
) -> crate::Result<Box<dyn CommandAction>> {
    use crate::parse::ParseError::EndOfStream;

    let key = parse.next_bytes()?;
    let option = match parse.next_string() {
        Ok(s) => match s.to_uppercase().as_str() {
            "PERSIST" => GetOpt::Persist,
            "EX" => {
                let secs = parse.next_int()?;
                GetOpt::Deadline(unix_timestamp_millis() + secs * 1000)
            }
            "PX" => {
                let ms = parse.next_int()?;
                GetOpt::Deadline(unix_timestamp_millis() + ms)
            }
            "EXAT" => GetOpt::Deadline(parse.next_int()? * 1000),
            "PXAT" => GetOpt::Deadline(parse.next_int()?),
            _ => {
                return Err("syntax error".into());
            }
        },
        Err(EndOfStream) => GetOpt::None,
        Err(err) => return Err(err.into()),
    };
    Ok(Box::new(Get { key, option }))
}

pub(crate) fn parse_range_frames(
    _: &CommandDescs,
    parse: &mut Parse,
) -> crate::Result<Box<dyn CommandAction>> {
    let key = parse.next_bytes()?;
    let start = parse.next_signed_int()?;
    let end = parse.next_signed_int()?;
    Ok(Box::new(Get {
        key,
        option: GetOpt::Range { start, end },
    }))
}

#[async_trait(?Send)]
impl CommandAction for Get {
    /// Apply the `Get` command to the specified `Db` instance.
    ///
    /// The response is written to `dst`. This is called by the server in order
    /// to execute a received command.
    async fn apply(&self, db: &Db) -> crate::Result<Frame> {
        // Get the value from the shared database state
        let response = if let Some(object_ref) = db.get(&self.key).await {
            if let Some(value) = object_ref.data::<RawString>() {
                let data = value.data_slice().to_vec();
                match self.option {
                    GetOpt::None => Frame::Bulk(data.into()),
                    GetOpt::Deadline(deadline) => {
                        db.update_deadline(&self.key, Some(deadline)).await;
                        Frame::Bulk(data.into())
                    }
                    GetOpt::Persist => {
                        db.update_deadline(&self.key, None).await;
                        Frame::Bulk(data.into())
                    }
                    GetOpt::Range { start, end } => {
                        let limited_index = |i| {
                            if i >= 0 {
                                std::cmp::min(i as usize, data.len())
                            } else {
                                std::cmp::max(data.len() as i64 + i, 0) as usize
                            }
                        };
                        let start = limited_index(start);
                        let end = limited_index(end);
                        if start < end {
                            let slice = data[start..end].to_owned();
                            Frame::Bulk(slice.into())
                        } else {
                            Frame::Bulk(Bytes::new())
                        }
                    }
                    GetOpt::Delete => {
                        db.remove(&self.key).await;
                        Frame::Bulk(data.into())
                    }
                }
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

    fn get_name(&self) -> &str {
        "get"
    }
}
