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

//! An Engula module that provides stateful environment abstractions and
//! implementations.
//!
//! # Abstraction
//!
//! [`Kernel`] is an abstraction to provide a stateful environment to storage
//! engines.
//!
//! # Implementation
//!
//! Some built-in implementations of [`Kernel`]:
//!
//! - [`mem`](crate::mem)
//!
//! [`Kernel`]: crate::Kernel

mod error;
mod kernel;
mod version;

pub mod mem;

pub use async_trait::async_trait;
pub use engula_journal::{Stream, Timestamp};
pub use engula_storage::Bucket;

pub type ResultStream<T> = Box<dyn futures::Stream<Item = Result<T>> + Send + Unpin>;

pub use self::{
    error::{Error, Result},
    kernel::{Kernel, KernelUpdate},
    version::{Sequence, Version, VersionUpdate},
};
