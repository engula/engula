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

mod get;
pub use get::Get;

mod set;
pub use set::Set;

mod ping;
pub use ping::Ping;

mod del;
pub use del::Del;

mod info;
pub use info::Info;

mod unknown;
use std::collections::HashMap;

mod cmds;
use cmds::all_cmd_tables;

mod command;

pub use unknown::Unknown;

use crate::{async_trait, Db, Frame, Parse, ParseError, Result};

pub struct Commands {
    commands: HashMap<String, CommandInfo>,
}

impl Commands {
    pub fn new() -> Self {
        Self {
            commands: all_cmd_tables(),
        }
    }

    pub fn lookup_command(&self, frame: Frame) -> crate::Result<Box<dyn Command>> {
        let mut parse = Parse::new(frame)?;
        let mut base_cmd_name = parse.next_string()?;
        base_cmd_name.make_ascii_lowercase();
        let base_cmd = self.commands.get(&base_cmd_name);
        if base_cmd.is_none() {
            return Ok(Box::new(Unknown::new(base_cmd_name)));
        }
        let base_cmd = base_cmd.unwrap();
        let command = if base_cmd.sub_cmds.is_empty() {
            (base_cmd.parse)(self, &mut parse)?
        } else {
            let mut sub_cmd = parse.next_string()?;
            sub_cmd.make_ascii_lowercase();
            let sub_cmd = base_cmd
                .sub_cmds
                .get(&sub_cmd)
                .ok_or_else(|| crate::Error::Unknown("illege command".to_string()))?;
            (sub_cmd.parse)(self, &mut parse)?
        };
        parse.finish()?;
        Ok(command)
    }
}

#[async_trait]
pub trait Command {
    async fn apply(&self, db: &Db) -> crate::Result<Frame>;
    fn get_name(&self) -> &str;
}

#[derive(Clone)]
pub struct CommandInfo {
    name: String,
    flags: u64,
    sub_cmds: HashMap<String, CommandInfo>,
    args: Vec<Arg>,
    tips: Vec<String>,
    group: Group,
    key_specs: Vec<KeySpec>,
    since: String,
    summary: String,

    parse: fn(&Commands, &mut Parse) -> Result<Box<dyn Command>>,
}

#[derive(Clone)]
pub enum Group {
    Generic,
    String,
    List,
    Set,
    SortedSet,
    Hash,
    Pubsub,
    Transactions,
    Connection,
    Server,
    Scripting,
    Hyperloglog,
    Cluster,
    Sentinel,
    Geo,
    Stream,
    Bitmap,
    Module,
}

#[derive(Clone)]
pub struct KeySpec {
    flags: u64,
    bs: BeginSearch,
    fk: FindKeys,
}

#[derive(Clone)]
pub enum BeginSearch {
    Index(usize),
    Keyword { keyword: String, start_from: usize },
}

#[derive(Clone)]
pub enum FindKeys {
    Range {
        last_key: usize,
        key_step: usize,
        limit: usize,
    },
    KeyNum {
        key_num_index: usize,
        first_key_index: usize,
        key_step: usize,
    },
}

#[derive(Clone)]
pub struct Arg {
    name: String,
    typ: ArgType,
    key_spec_index: usize,
    token: String,
    flag: u64,
    sub_args: Vec<Arg>,

    since: String,
    summary: String,
}

#[derive(Clone)]
pub enum ArgType {
    String,
    Int,
    Double,
    Key, /* A string, but represents a keyname */
    Pattern,
    UnixTime,
    PureToken,
    OneOf, /* Has subargs */
    Block,
}

#[allow(dead_code)]
const CMD_ARG_NONE: u8 = 0;
#[allow(dead_code)]
const CMD_ARG_OPTIONAL: u8 = 1 << 0;
#[allow(dead_code)]
const CMD_ARG_MULTIPLE: u8 = 1 << 1;
#[allow(dead_code)]
const CMD_ARG_MULTIPLE_TOKEN: u8 = 1 << 2;
