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

mod action;
mod group;

pub use self::{
    action::ActionTask,
    group::{CureGroup, RemoveOrphanReplica, PromoteGroup},
};

pub const PROMOTE_GROUP_TASK_ID: u64 = 1;
pub const CURE_GROUP_TASK_ID: u64 = 2;
pub const REMOVE_ORPHAN_REPLICA_TASK_ID: u64 = 3;

pub const GENERATED_TASK_ID: u64 = 10;
