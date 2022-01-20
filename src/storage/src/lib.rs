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

//! Object storage abstractions and implementations.
//!
//! # Abstraction
//!
//! [`Storage`] is an object storage abstraction.
//!
//! # Implementation
//!
//! Built-in implementations of [`Storage`]:
//!
//! - [`MemStorage`](crate::MemStorage)
//!
//! [`Storage`]: crate::Storage

#![feature(type_alias_impl_trait)]

mod error;
mod external;
mod local;
mod remote;
mod storage;

pub use async_trait::async_trait;

pub use self::{
    error::{Error, Result},
    external::S3Storage,
    local::MemStorage,
    remote::CachedStorage,
    storage::{Storage, WriteOption},
};
