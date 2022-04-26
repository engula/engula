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

#![feature(ptr_as_uninit)]
#![allow(clippy::missing_safety_doc)]

pub mod alloc;
mod compact;
mod db;
pub mod elements;
mod key_space;
pub mod objects;
pub mod record;

pub use self::{alloc::read_mem_stats, compact::migrate_record, db::Db};
