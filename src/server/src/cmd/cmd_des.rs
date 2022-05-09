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
        let command = if frame_cnt == 1 || base_cmd.sub_cmds.is_none() {
            (base_cmd.parse)(self, &mut parse)?
        } else {
            let mut sub_cmd_name = parse.next_string()?;
            sub_cmd_name.make_ascii_lowercase();
            if base_cmd.sub_cmds.is_none() {
                return Ok(Box::new(Unknown::new(base_cmd_name + "|" + &sub_cmd_name)));
            }
            let sub_cmds = base_cmd.sub_cmds.as_ref().unwrap();
            let sub_cmd = sub_cmds.get(&sub_cmd_name);
            if sub_cmd.is_none() {
                return Ok(Box::new(Unknown::new(base_cmd_name + "|" + &sub_cmd_name)));
            }
            let sub_cmd = sub_cmd.unwrap();
            (sub_cmd.parse)(self, &mut parse)?
        };
        parse.finish()?;
        Ok(command)
    }
}

#[async_trait(?Send)]
pub trait CommandAction {
    async fn apply(&self, db: &Db) -> crate::Result<Frame>;
    fn get_name(&self) -> &str;
}

#[derive(Clone)]
pub struct CommandDesc {
    pub name: String,
    pub arity: i64, // -N means ">= N"
    pub flags: u64,
    pub sub_cmds: Option<HashMap<String, CommandDesc>>,
    pub args: Vec<Arg>,
    pub tips: Vec<String>,
    pub group: Group,
    pub key_specs: Vec<KeySpec>,
    pub since: String,
    pub summary: String,

    pub complexity: String,
    pub doc_flags: u64,
    pub acl_categories: u64,
    pub deprecated_since: String,
    pub replaced_by: String,

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
    pub notes: String,
    pub flags: u64,
    pub bs: BeginSearch,
    pub fk: FindKeys,
}

#[derive(Clone)]
pub enum BeginSearch {
    Unknown,
    Index(i64),
    Keyword { keyword: String, start_from: i64 },
}

#[derive(Clone)]
pub enum FindKeys {
    Unknown,
    Range {
        last_key: i64,
        key_step: i64,
        limit: i64,
    },
    KeyNum {
        key_num_index: i64,
        first_key_index: i64,
        key_step: i64,
    },
}

#[derive(Clone, Default)]
pub struct Arg {
    pub name: String,
    pub typ: ArgType,
    pub key_spec_index: i64,
    pub token: String,
    pub flag: u64,
    pub sub_args: Vec<Arg>,

    pub since: String,
    pub summary: String,
    pub deprecated_since: String,
}

#[derive(Clone)]
pub enum ArgType {
    String,
    Integer,
    Double,
    Key, /* A string, but represents a keyname */
    Pattern,
    UnixTime,
    PureToken,
    OneOf, /* Has subargs */
    Block,
}

impl Default for ArgType {
    fn default() -> Self {
        ArgType::String
    }
}

// Command Arg Flags.
pub const CMD_ARG_NONE: u64 = 0;
pub const CMD_ARG_OPTIONAL: u64 = 1 << 0;
pub const CMD_ARG_MULTIPLE: u64 = 1 << 1;
pub const CMD_ARG_MULTIPLE_TOKEN: u64 = 1 << 2;

// Command Flags
pub const CMD_WRITE: u64 = 1 << 0;
pub const CMD_READONLY: u64 = 1 << 1;
pub const CMD_DENYOOM: u64 = 1 << 2;
pub const CMD_MODULE: u64 = 1 << 3; /* Command exported by module. */
pub const CMD_ADMIN: u64 = 1 << 4;
pub const CMD_PUBSUB: u64 = 1 << 5;
pub const CMD_NOSCRIPT: u64 = 1 << 6;
pub const CMD_BLOCKING: u64 = 1 << 8; /* Has potential to block. */
pub const CMD_LOADING: u64 = 1 << 9;
pub const CMD_STALE: u64 = 1 << 10;
pub const CMD_SKIP_MONITOR: u64 = 1 << 11;
pub const CMD_SKIP_SLOWLOG: u64 = 1 << 12;
pub const CMD_ASKING: u64 = 1 << 13;
pub const CMD_FAST: u64 = 1 << 14;
pub const CMD_NO_AUTH: u64 = 1 << 15;
pub const CMD_MAY_REPLICATE: u64 = 1 << 16;
pub const CMD_SENTINEL: u64 = 1 << 17;
pub const CMD_ONLY_SENTINEL: u64 = 1 << 18;
pub const CMD_NO_MANDATORY_KEYS: u64 = 1 << 19;
pub const CMD_PROTECTED: u64 = 1 << 20;
pub const CMD_MODULE_GETKEYS: u64 = 1 << 21; /* Use the modules getkeys interface. */
pub const CMD_MODULE_NO_CLUSTER: u64 = 1 << 22; /* Deny on Redis Cluster. */
pub const CMD_NO_ASYNC_LOADING: u64 = 1 << 23;
pub const CMD_NO_MULTI: u64 = 1 << 24;
pub const CMD_MOVABLE_KEYS: u64 = 1 << 25; /* populated by populateCommandMovableKeys */
pub const CMD_ALLOW_BUSY: u64 = 1 << 26;
pub const CMD_MODULE_GETCHANNELS: u64 = 1 << 27;

pub fn cmd_flag_name(flag: u64) -> Vec<String> {
    let flags = [
        (CMD_WRITE, "write"),
        (CMD_READONLY, "readonly"),
        (CMD_DENYOOM, "denyoom"),
        (CMD_MODULE, "module"),
        (CMD_ADMIN, "admin"),
        (CMD_PUBSUB, "pubsub"),
        (CMD_NOSCRIPT, "noscript"),
        (CMD_BLOCKING, "blocking"),
        (CMD_LOADING, "loading"),
        (CMD_STALE, "stale"),
        (CMD_SKIP_MONITOR, "skip_monitor"),
        (CMD_SKIP_SLOWLOG, "skip_slowlog"),
        (CMD_ASKING, "asking"),
        (CMD_FAST, "fast"),
        (CMD_NO_AUTH, "no_auth"),
        (CMD_MAY_REPLICATE, "may_replicate"),
        (CMD_NO_MANDATORY_KEYS, "no_mandatory_keys"),
        (CMD_NO_ASYNC_LOADING, "no_async_loading"),
        (CMD_NO_MULTI, "no_multi"),
        (CMD_MOVABLE_KEYS, "movablekeys"),
        (CMD_ALLOW_BUSY, "allow_busy"),
    ];
    flags
        .into_iter()
        .filter(|f| f.0 & flag > 0)
        .map(|f| f.1.to_owned())
        .collect()
}

// Key Spec FLags.
// TODO: add comments
pub const CMD_KEY_RO: u64 = 1 << 0;
pub const CMD_KEY_RW: u64 = 1 << 1;
pub const CMD_KEY_OW: u64 = 1 << 2;
pub const CMD_KEY_RM: u64 = 1 << 3;
pub const CMD_KEY_ACCESS: u64 = 1 << 4;
pub const CMD_KEY_UPDATE: u64 = 1 << 5;
pub const CMD_KEY_INSERT: u64 = 1 << 6;
pub const CMD_KEY_DELETE: u64 = 1 << 7;
pub const CMD_KEY_NOT_KEY: u64 = 1 << 8;
pub const CMD_KEY_INCOMPLETE: u64 = 1 << 9;
pub const CMD_KEY_VARIABLE_FLAGS: u64 = 1 << 10;
pub const CMD_KEY_FULL_ACCESS: u64 = CMD_KEY_RW | CMD_KEY_ACCESS | CMD_KEY_UPDATE;

pub fn keyspec_flag_name(flag: u64) -> Vec<String> {
    let flags = [
        (CMD_KEY_RO, "RO"),
        (CMD_KEY_RW, "RW"),
        (CMD_KEY_OW, "OW"),
        (CMD_KEY_RM, "RM"),
        (CMD_KEY_ACCESS, "access"),
        (CMD_KEY_UPDATE, "update"),
        (CMD_KEY_INSERT, "insert"),
        (CMD_KEY_DELETE, "delete"),
        (CMD_KEY_NOT_KEY, "not_key"),
        (CMD_KEY_INCOMPLETE, "incomplete"),
        (CMD_KEY_VARIABLE_FLAGS, "variable_flags"),
    ];
    flags
        .into_iter()
        .filter(|f| f.0 & flag > 0)
        .map(|f| f.1.to_owned())
        .collect()
}

// Doc Flags.
pub const CMD_DOC_NONE: u64 = 0;
pub const CMD_DOC_DEPRECATED: u64 = 1 << 0; /* Command is deprecated */
pub const CMD_DOC_SYSCMD: u64 = 1 << 1; /* System (internal) command */

// ACL Flags.
pub const ACL_CATEGORY_KEYSPACE: u64 = 1 << 0;
pub const ACL_CATEGORY_READ: u64 = 1 << 1;
pub const ACL_CATEGORY_WRITE: u64 = 1 << 2;
pub const ACL_CATEGORY_SET: u64 = 1 << 3;
pub const ACL_CATEGORY_SORTEDSET: u64 = 1 << 4;
pub const ACL_CATEGORY_LIST: u64 = 1 << 5;
pub const ACL_CATEGORY_HASH: u64 = 1 << 6;
pub const ACL_CATEGORY_STRING: u64 = 1 << 7;
pub const ACL_CATEGORY_BITMAP: u64 = 1 << 8;
pub const ACL_CATEGORY_HYPERLOGLOG: u64 = 1 << 9;
pub const ACL_CATEGORY_GEO: u64 = 1 << 10;
pub const ACL_CATEGORY_STREAM: u64 = 1 << 11;
pub const ACL_CATEGORY_PUBSUB: u64 = 1 << 12;
pub const ACL_CATEGORY_ADMIN: u64 = 1 << 13;
pub const ACL_CATEGORY_FAST: u64 = 1 << 14;
pub const ACL_CATEGORY_SLOW: u64 = 1 << 15;
pub const ACL_CATEGORY_BLOCKING: u64 = 1 << 16;
pub const ACL_CATEGORY_DANGEROUS: u64 = 1 << 17;
pub const ACL_CATEGORY_CONNECTION: u64 = 1 << 18;
pub const ACL_CATEGORY_TRANSACTION: u64 = 1 << 19;
pub const ACL_CATEGORY_SCRIPTING: u64 = 1 << 20;
