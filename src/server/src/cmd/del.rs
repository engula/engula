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
use tracing::{debug, instrument};

use crate::{Connection, Db, Error, Frame, Parse, ParseError};

/// Delete the given keys.
#[derive(Debug)]
pub struct Del {
    keys: Vec<String>,
}

impl Del {
    pub fn new(keys: Vec<String>) -> Del {
        Del { keys }
    }

    pub fn keys(&self) -> &Vec<String> {
        &self.keys
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Del> {
        let mut keys = vec![];
        loop {
            match parse.next_string() {
                Ok(key) => keys.push(key),
                Err(ParseError::EndOfStream) => break,
                Err(err) => return Err(err.into()),
            }
        }

        if keys.len() > 0 {
            Ok(Del { keys })
        } else {
            Err(Error::from("Del without keys"))
        }
    }

    #[instrument(skip(self, db, dst))]
    pub(crate) async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
        // Get the value from the shared database state
        let response = Frame::Integer(db.del(&self.keys));

        debug!(?response);

        // Write the response back to the client
        dst.write_frame(&response).await?;

        Ok(())
    }

    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("del".as_bytes()));
        for key in self.keys.into_iter() {
            frame.push_bulk(Bytes::from(key.into_bytes()))
        }
        frame
    }
}
