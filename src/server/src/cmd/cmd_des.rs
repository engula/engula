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

use std::collections::HashMap;

use super::*;
use crate::{async_trait, Db, Frame, Parse, Result};

pub struct CommandDescs {
    pub commands: HashMap<String, CommandDesc>,
}

impl CommandDescs {
    pub fn new() -> Self {
        Self {
            commands: all_cmd_tables(),
        }
    }

    pub fn lookup_command(
        &self,
        frame: Frame,
        frame_cnt: usize,
    ) -> crate::Result<Box<dyn CommandAction>> {
        let mut parse = Parse::new(frame)?;
        let mut base_cmd_name = parse.next_string()?;
        base_cmd_name.make_ascii_lowercase();
        let base_cmd = self.commands.get(&base_cmd_name);
        if base_cmd.is_none() {
            return Ok(Box::new(Unknown::new(base_cmd_name)));
        }
        let base_cmd = base_cmd.unwrap();
        let command = if frame_cnt == 1 || base_cmd.sub_cmds.is_empty() {
            (base_cmd.parse)(self, &mut parse)?
        } else {
            let mut sub_cmd_name = parse.next_string()?;
            sub_cmd_name.make_ascii_lowercase();
            let sub_cmd = base_cmd.sub_cmds.get(&sub_cmd_name);
            if sub_cmd.is_none() {
                return Ok(Box::new(Unknown::new(base_cmd_name + " " + &sub_cmd_name)));
            }
            let sub_cmd = sub_cmd.unwrap();
            (sub_cmd.parse)(self, &mut parse)?
        };
        parse.finish()?;
        Ok(command)
    }
}

#[async_trait]
pub trait CommandAction {
    async fn apply(&self, db: &Db) -> crate::Result<Frame>;
    fn get_name(&self) -> &str;
}

#[derive(Clone)]
pub struct CommandDesc {
    pub name: String,
    pub flags: u64,
    pub sub_cmds: HashMap<String, CommandDesc>,
    pub args: Vec<Arg>,
    pub tips: Vec<String>,
    pub group: Group,
    pub key_specs: Vec<KeySpec>,
    pub since: String,
    pub summary: String,

    pub parse: fn(&CommandDescs, &mut Parse) -> Result<Box<dyn CommandAction>>,
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
