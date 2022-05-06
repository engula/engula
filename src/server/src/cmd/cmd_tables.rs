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

use std::{collections::HashMap, vec};

use super::*;

// TODO: use marco to generate this command tables.
pub fn all_cmd_tables() -> HashMap<String, CommandDesc> {
    let cmds = [
        // string
        CommandDesc {
            name: "get".to_string(),
            flags: CMD_FLAG_FAST | CMD_FLAG_READONLY,
            sub_cmds: None,
            arity: 2,
            args: vec![Arg{ name: "key".to_string(), typ: ArgType::Key, key_spec_index: 0, ..Default::default() }],
            tips: vec![],
            group: Group::String,
            key_specs: vec![KeySpec{ flags: CMD_KEY_SPEC_FLAG_RO | CMD_KEY_SPEC_FLAG_ACCESS, bs: BeginSearch::Index(1), fk: FindKeys::Range { last_key: 0, key_step: 1, limit: 0 } }],
            since: "1.0.0".to_string(),
            summary: "Get the value of a key".to_string(),
            parse: get::parse_frames,
        },
        CommandDesc {
            name: "set".to_string(),
            arity: -3,
            flags: CMD_FLAG_DENYOOM | CMD_FLAG_WRITE,
            sub_cmds: None,
            args: vec![],
            tips: vec![],
            group: Group::String,
            key_specs: vec![],
            since: "1.0.0".to_string(),
            summary: "Set the string value of a key".to_string(),
            parse: set::parse_frames,
        },
        CommandDesc {
            name: "del".to_string(),
            arity: -2,
            flags: CMD_FLAG_WRITE,
            sub_cmds: None,
            args: vec![Arg{
                name: "key".to_string(),
                typ: ArgType::Key,
                key_spec_index: 0,
                multiple: true,
                ..Default::default()
            }],
            tips: vec![
                "REQUEST_POLICY:MULTI_SHARD".to_string(),
                "RESPONSE_POLICY:AGG_SUM".to_string(),
            ],
            group: Group::String,
            key_specs: vec![KeySpec{ flags: CMD_KEY_SPEC_FLAG_RM | CMD_KEY_SPEC_FLAG_DELETE, bs: BeginSearch::Index(1), fk: FindKeys::Range { last_key: 0, key_step: 1, limit: 0 } }], // TODO: last_key=-1
            since: "1.0.0".to_string(),
            summary: "Delete a key".to_string(),
            parse: del::parse_frames,
        },
        // connection
        CommandDesc {
            name: "ping".to_string(),
            arity: -1,
            flags: CMD_FLAG_FAST | CMD_FLAG_SENTINEL,
            sub_cmds: None,
            args: vec![Arg{ name: "message".to_owned(), typ: ArgType::String, optional: true, ..Default::default()}],
            tips: vec![
                "REQUEST_POLICY:ALL_SHARDS".to_string(),
                "RESPONSE_POLICY:ALL_SUCCEEDED".to_string(),
            ],
            group: Group::Connection,
            key_specs: vec![],
            since: "1.0.0".to_string(),
            summary: "Ping the server".to_string(),
            parse: ping::parse_frames,
        },
        // server
        CommandDesc {
            name: "info".to_string(),
            arity: -1,
            flags: CMD_FLAG_LOADING | CMD_FLAG_STALE | CMD_FLAG_SENTINEL,
            sub_cmds: None,
            args: vec![],
            tips: vec![
                "NONDETERMINISTIC_OUTPUT".to_string(),
                "REQUEST_POLICY:ALL_SHARDS".to_string(),
                "RESPONSE_POLICY:SPECIAL".to_string(),
            ],
            group: Group::Server,
            key_specs: vec![],
            since: "1.0.0".to_string(),
            summary: "Get information and statistics about the server".to_string(),
            parse: info::parse_frames,
        },
        CommandDesc {
            name: "command".to_string(),
            arity: -1,
            flags: CMD_FLAG_LOADING | CMD_FLAG_STALE | CMD_FLAG_SENTINEL,
            sub_cmds: Some(vec![(
                "info".to_string(),
                CommandDesc {
                    name: "info".to_string(),
                    arity: -2,
                    flags: CMD_FLAG_LOADING | CMD_FLAG_STALE,
                    sub_cmds: None,
                    args: vec![Arg{ name: "command-name".to_string(), typ: ArgType::String, optional: true, multiple: true, ..Default::default() }],
                    tips: vec!["NONDETERMINISTIC_OUTPUT_ORDER".to_string()],
                    group: Group::Server,
                    key_specs: vec![],
                    since: "1.0.0".to_string(),
                    summary: "Get array of specific Engula command details, or all when no argument is given.".to_string(),
                    parse: command::parse_command_info,
                },
            )]
            .into_iter()
            .collect()),
            args: vec![],
            tips: vec!["NONDETERMINISTIC_OUTPUT_ORDER".to_string()],
            group: Group::Server,
            key_specs: vec![],
            since: "1.0.0".to_string(),
            summary: "Get array of Engula command details".to_string(),
            parse: command::parse_command,
        },
    ];
    cmds.into_iter()
        .map(|e| (e.name.clone(), e))
        .collect::<HashMap<String, CommandDesc>>()
}
