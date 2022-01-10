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
mod metadata;
mod update_builder;

mod local;

pub use async_trait::async_trait;
pub use engula_journal::Sequence;

pub use self::{
    error::{Error, Result},
    kernel::{Kernel, UpdateEvent, UpdateReader, UpdateWriter},
    local::MemKernel,
    metadata::{BucketUpdate, KernelUpdate},
    update_builder::{BucketUpdateBuilder, KernelUpdateBuilder},
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

    async fn test_kernel<K>(kernel: K) -> Result<()>
    where
        K: Kernel,
        K::UpdateReader: Send + 'static,
    {
        let stream_name = "stream";
        let bucket_name = "bucket";
        let object_name = "object";

        let update = KernelUpdateBuilder::default()
            .put_meta("a", "b")
            .remove_meta("b")
            .add_stream(stream_name)
            .add_bucket(bucket_name)
            .build();

        let handle = {
            let expect = update.clone();
            let mut reader = kernel.new_update_reader().await?;
            tokio::spawn(async move {
                let update = reader.wait_next().await.unwrap();
                assert_eq!(update, (1, expect));
            })
        };

        let mut writer = kernel.new_update_writer().await?;
        writer.append(update).await?;
        handle.await.unwrap();

        // Checks if the stream and bucket have been created.
        kernel.new_stream_writer(stream_name).await.unwrap();
        kernel
            .new_sequential_writer(bucket_name, object_name)
            .await
            .unwrap();
        Ok(())
    }
}
