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

use super::*;
use crate::{async_trait, Db, Frame, Parse};

pub struct Command {
    cmds: Vec<CommandDesc>,
}

pub(crate) fn parse_command(
    cmds: &CommandDescs,
    parse: &mut Parse,
) -> crate::Result<Box<dyn super::CommandAction>> {
    match parse.finish() {
        Ok(()) => Ok(Box::new(Command {
            cmds: cmds.commands.values().cloned().collect(),
        })),
        Err(_) => Err("wrong number of arguments for 'command' command".into()),
    }
}

#[async_trait]
impl CommandAction for Command {
    async fn apply(&self, _: &Db) -> crate::Result<Frame> {
        let mut cmds = Vec::new();
        for desc in &self.cmds {
            cmds.push(cmd_to_frame(desc));
        }
        Ok(Frame::Array(cmds))
    }

    fn get_name(&self) -> &str {
        "command"
    }
}

pub struct CommandInfo {
    cmd: Option<CommandDesc>,
}

pub(crate) fn parse_command_info(
    cmds: &CommandDescs,
    parse: &mut Parse,
) -> crate::Result<Box<dyn super::CommandAction>> {
    let sub_cmd_name = parse.next_string()?;
    match parse.finish() {
        Ok(()) => {
            let cmd = cmds.commands.get(&sub_cmd_name).cloned();
            Ok(Box::new(CommandInfo { cmd }))
        }
        Err(_) => Err("wrong number of arguments for 'command info' command".into()),
    }
}

#[async_trait]
impl CommandAction for CommandInfo {
    async fn apply(&self, _: &Db) -> crate::Result<Frame> {
        let mut cmds = Vec::new();
        if let Some(desc) = &self.cmd {
            cmds.push(cmd_to_frame(desc));
        }
        Ok(Frame::Array(cmds))
    }

    fn get_name(&self) -> &str {
        "command info"
    }
}

fn cmd_to_frame(desc: &CommandDesc) -> Frame {
    let first_key = 0;
    let last_key = 0;
    let key_step = 0;
    let cmd_frames = vec![
        Frame::Bulk(desc.name.to_owned().into()),
        Frame::Integer(desc.args.len() as u64),
        Frame::Array(
            cmd_flag_name(desc.flags)
                .iter()
                .map(|f| Frame::Bulk(f.to_owned().into()))
                .collect(),
        ),
        Frame::Integer(first_key),
        Frame::Integer(last_key),
        Frame::Integer(key_step),
        Frame::Array(vec![]), // TODO: acl_category
        Frame::Array(
            desc.tips
                .iter()
                .map(|f| Frame::Bulk(f.to_lowercase().into()))
                .collect(),
        ),
        Frame::Array(desc.key_specs.iter().map(keyspec_to_frame).collect()),
        Frame::Array(vec![]), // TODO: subcommands
    ];
    Frame::Array(cmd_frames)
}

fn keyspec_to_frame(spec: &KeySpec) -> Frame {
    let mut frames = Vec::new();
    frames.extend(vec![
        Frame::Bulk("flags".into()),
        Frame::Array(
            keyspec_flag_name(spec.flags)
                .iter()
                .map(|f| Frame::Bulk(f.to_owned().into()))
                .collect(),
        ),
        Frame::Bulk("begin_search".into()),
        Frame::Bulk("type".into()),
    ]);
    match &spec.bs {
        BeginSearch::Index(idx) => {
            frames.push(Frame::Bulk("index".into()));
            frames.push(Frame::Bulk("spec".into()));
            frames.push(Frame::Array(vec![
                Frame::Bulk("index".into()),
                Frame::Integer(idx.to_owned()),
            ]));
        }
        BeginSearch::Keyword {
            keyword,
            start_from,
        } => {
            frames.push(Frame::Bulk("keyword".into()));
            frames.push(Frame::Bulk("spec".into()));
            frames.push(Frame::Array(vec![
                Frame::Bulk("keyword".into()),
                Frame::Bulk(keyword.to_owned().into()),
                Frame::Bulk("startfrom".into()),
                Frame::Integer(start_from.to_owned()),
            ]));
        }
    };
    frames.push(Frame::Bulk("find_keys".into()));
    frames.push(Frame::Bulk("type".into()));
    match &spec.fk {
        FindKeys::Range {
            last_key,
            key_step,
            limit,
        } => {
            frames.push(Frame::Bulk("range".into()));
            frames.push(Frame::Bulk("spec".into()));
            frames.push(Frame::Array(vec![
                Frame::Bulk("lastkey".into()),
                Frame::Integer(last_key.to_owned()),
                Frame::Bulk("keystep".into()),
                Frame::Integer(key_step.to_owned()),
                Frame::Bulk("limit".into()),
                Frame::Integer(limit.to_owned()),
            ]));
        }
        FindKeys::KeyNum {
            key_num_index,
            first_key_index,
            key_step,
        } => {
            frames.push(Frame::Bulk("keynum".into()));
            frames.push(Frame::Bulk("spec".into()));
            frames.push(Frame::Array(vec![
                Frame::Bulk("keynumidx".into()),
                Frame::Integer(key_num_index.to_owned()),
                Frame::Bulk("firstkey".into()),
                Frame::Integer(first_key_index.to_owned()),
                Frame::Bulk("keystep".into()),
                Frame::Integer(key_step.to_owned()),
            ]));
        }
    }
    Frame::Array(frames)
}
