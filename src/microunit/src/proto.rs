// Copyright 2021 The Engula Authors.
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

use std::cmp::PartialEq;

use serde::{Deserialize, Serialize};

/// Describes the current state of a node.
#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
pub struct NodeDesc {}

pub type NodeDescList = Vec<NodeDesc>;

/// Describes the specification of a unit.
#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
pub struct UnitSpec {
    pub kind: String,
}

/// Describes the current state of a unit.
#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
pub struct UnitDesc {
    pub id: String,
    pub kind: String,
    pub addr: Option<String>,
}

pub type UnitDescList = Vec<UnitDesc>;

/// Describes the current state of a control unit.
#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
pub struct ControlDesc {}
