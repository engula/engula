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
use mio::{event::Event, net::TcpStream, Token};

use crate::{ReadBuf, Result, WriteBuf};

pub struct Connection {
    id: Token,
    db: Db,
    stream: TcpStream,
    read_buf: ReadBuf,
    write_buf: WriteBuf,
}

impl Connection {
    pub fn new(id: Token, db: Db, stream: TcpStream) -> Connection {
        Self {
            id,
            db,
            read_buf: ReadBuf::default(),
            write_buf: WriteBuf::default(),
            stream,
        }
    }

    pub fn stream(&mut self) -> &mut TcpStream {
        &mut self.stream
    }

    pub fn handle_connection_event(&mut self, event: &Event) -> Result<bool> {
        Ok(false)
    }
}
