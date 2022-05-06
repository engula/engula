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

// TODO: use marco to generate this command tables.
pub fn all_cmd_tables() -> HashMap<String, CommandDesc> {
    let cmds = [
        CommandDesc {
            name: "get".to_string(),
            flags: 0,
            sub_cmds: HashMap::new(),
            args: Vec::new(),
            tips: Vec::new(),
            group: Group::String,
            key_specs: Vec::new(),
            since: "1.0.0".to_string(),
            summary: "".to_string(),
            parse: get::parse_frames,
        },
        CommandDesc {
            name: "set".to_string(),
            flags: 0,
            sub_cmds: HashMap::new(),
            args: Vec::new(),
            tips: Vec::new(),
            group: Group::String,
            key_specs: Vec::new(),
            since: "1.0.0".to_string(),
            summary: "".to_string(),
            parse: set::parse_frames,
        },
        CommandDesc {
            name: "del".to_string(),
            flags: 0,
            sub_cmds: HashMap::new(),
            args: Vec::new(),
            tips: Vec::new(),
            group: Group::String,
            key_specs: Vec::new(),
            since: "1.0.0".to_string(),
            summary: "".to_string(),
            parse: del::parse_frames,
        },
        CommandDesc {
            name: "ping".to_string(),
            flags: 0,
            sub_cmds: HashMap::new(),
            args: Vec::new(),
            tips: Vec::new(),
            group: Group::Connection,
            key_specs: Vec::new(),
            since: "1.0.0".to_string(),
            summary: "".to_string(),
            parse: ping::parse_frames,
        },
        CommandDesc {
            name: "info".to_string(),
            flags: 0,
            sub_cmds: HashMap::new(),
            args: Vec::new(),
            tips: Vec::new(),
            group: Group::Server,
            key_specs: Vec::new(),
            since: "1.0.0".to_string(),
            summary: "".to_string(),
            parse: info::parse_frames,
        },
        CommandDesc {
            name: "command".to_string(),
            flags: 0,
            sub_cmds: vec![(
                "info".to_string(),
                CommandDesc {
                    name: "info".to_string(),
                    flags: 0,
                    sub_cmds: HashMap::new(),
                    args: Vec::new(),
                    tips: Vec::new(),
                    group: Group::Server,
                    key_specs: Vec::new(),
                    since: "1.0.0".to_string(),
                    summary: "".to_string(),
                    parse: command::parse_command_info,
                },
            )]
            .into_iter()
            .collect(),
            args: Vec::new(),
            tips: Vec::new(),
            group: Group::Server,
            key_specs: Vec::new(),
            since: "1.0.0".to_string(),
            summary: "".to_string(),
            parse: command::parse_command,
        },
    ];
    cmds.into_iter()
        .map(|e| (e.name.clone(), e))
        .collect::<HashMap<String, CommandDesc>>()
}
