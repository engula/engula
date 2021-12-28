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
//! - [`file`](crate::file)
//! - [`grpc`](crate::grpc)
//!
//! [`Kernel`]: crate::Kernel

mod error;
mod kernel;
mod kernel_update;
mod manifest;
mod metadata;

// pub mod file;
// pub mod grpc;
mod local;
pub mod mem;

pub use async_trait::async_trait;
pub use engula_journal::{Event, Journal, StreamRead, StreamWrite, Timestamp};
pub use engula_storage::Storage;

pub use self::{
    error::{Error, Result},
    kernel::{Kernel, VersionUpdateStream},
    kernel_update::KernelUpdate,
    metadata::{Sequence, Version, VersionUpdate},
};

#[cfg(test)]
mod tests {
    use crate::*;

    #[tokio::test]
    async fn kernel() -> Result<()> {
        let kernel = mem::Kernel::open().await?;
        test_kernel(kernel).await?;

        Ok(())
    }

    async fn test_kernel(kernel: impl Kernel<u64>) -> Result<()> {
        let handle = {
            let mut expect = VersionUpdate {
                sequence: 1,
                ..Default::default()
            };
            expect.put_meta.insert("a".to_owned(), b"b".to_vec());
            expect.remove_meta.push("b".to_owned());
            let mut update_stream = kernel.subscribe_version_updates().await?;
            tokio::spawn(async move {
                let update = update_stream.next().await.unwrap();
                assert_eq!(update, expect);
            })
        };

        let mut update = KernelUpdate::default();
        update.put_meta("a", "b");
        update.remove_meta("b");
        kernel.apply_update(update).await?;

        handle.await.unwrap();
        Ok(())
    }
}
