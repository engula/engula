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

use super::{CommandInfo, Commands};
use crate::{async_trait, Db, Frame, Parse};

pub struct Command {
    cmds: Vec<CommandInfo>,
}

pub(crate) fn parse_frames(
    cmds: &Commands,
    parse: &mut Parse,
) -> crate::Result<Box<dyn super::Command>> {
    // TODO: support command subcommand
    match parse.finish() {
        Ok(()) => Ok(Box::new(Command {
            cmds: cmds.commands.values().cloned().collect(),
        })),
        Err(_) => Err("wrong number of arguments for 'command' command".into()),
    }
}

#[async_trait]
impl super::Command for Command {
    async fn apply(&self, _: &Db) -> crate::Result<Frame> {
        let mut cmds = Vec::new();
        for info in &self.cmds {
            let first_key = 0;
            let last_key = 0;
            let key_step = 0;
            let cmd_frames = vec![
                Frame::Bulk(info.name.to_owned().into()),
                Frame::Integer(info.args.len() as u64),
                Frame::Array(vec![]), // TODO: flags
                Frame::Integer(first_key),
                Frame::Integer(last_key),
                Frame::Integer(key_step),
                Frame::Array(vec![]), // TODO: category
                Frame::Array(vec![]), // TODO: tips
                Frame::Array(vec![]), // TODO: key_specs
                Frame::Array(vec![]), // TODO: subcommands
            ];
            cmds.push(Frame::Array(cmd_frames));
        }
        Ok(Frame::Array(cmds))
    }

    fn get_name(&self) -> &str {
        "command"
    }
}
