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

pub struct Token(pub u64);

impl Token {
    pub fn new(id: u64, op: u8) -> Token {
        Token(id << 8 | op as u64)
    }

    pub fn id(token: u64) -> u64 {
        token >> 8
    }

    pub fn op(token: u64) -> u8 {
        token as u8
    }
}
