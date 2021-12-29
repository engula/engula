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
//! [`Kernel`]: crate::Kernel

mod error;
mod kernel;
mod kernel_update;
mod metadata;

mod local;

pub use async_trait::async_trait;
pub use engula_journal::{Event, Journal, StreamReader, StreamWriter, Timestamp};
pub use engula_storage::Storage;

pub use self::{
    error::{Error, Result},
    kernel::Kernel,
    kernel_update::{BucketUpdateBuilder, KernelUpdateBuilder, KernelUpdateReader},
    local::MemKernel,
    metadata::{BucketUpdate, KernelUpdate, Sequence},
};

#[cfg(test)]
mod tests {
    use crate::*;

    #[tokio::test]
    async fn kernel() -> Result<()> {
        let kernel = MemKernel::open().await?;
        test_kernel(kernel).await?;
        Ok(())
    }

    async fn test_kernel(kernel: impl Kernel<u64>) -> Result<()> {
        let update = KernelUpdateBuilder::default()
            .put_meta("a", "b")
            .remove_meta("b")
            .build();

        let handle = {
            let mut expect = update.clone();
            expect.sequence = 1;
            let mut update_reader = kernel.new_update_reader().await?;
            tokio::spawn(async move {
                let update = update_reader.wait_next().await.unwrap();
                assert_eq!(update, expect);
            })
        };

        kernel.apply_update(update).await?;
        handle.await.unwrap();
        Ok(())
    }
}
