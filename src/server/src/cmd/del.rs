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
use tracing::debug;

use super::*;
use crate::{async_trait, Db, Error, Frame, Parse, ParseError};

/// Delete the given keys.
#[derive(Debug)]
pub struct Del {
    keys: Vec<Bytes>,
}

pub(crate) fn parse_frames(
    _: &CommandDescs,
    parse: &mut Parse,
) -> crate::Result<Box<dyn CommandAction>> {
    let mut keys = vec![];
    loop {
        match parse.next_bytes() {
            Ok(key) => keys.push(key),
            Err(ParseError::EndOfStream) => break,
            Err(err) => return Err(err.into()),
        }
    }

    if !keys.is_empty() {
        Ok(Box::new(Del { keys }))
    } else {
        Err(Error::from("Del without keys"))
    }
}

impl Del {
    pub fn new(keys: Vec<Bytes>) -> Del {
        Del { keys }
    }
}

#[async_trait]
impl CommandAction for Del {
    async fn apply(&self, db: &Db) -> crate::Result<Frame> {
        // Get the value from the shared database state
        let response = Frame::Integer(db.delete_keys(self.keys.iter().map(|bytes| bytes.as_ref())));

        debug!(?response);

        Ok(response)
    }

    fn get_name(&self) -> &str {
        "del"
    }
}
