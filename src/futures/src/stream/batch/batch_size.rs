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

use super::{ResultStream, ResultStreamExt};

pub struct BatchSize<T> {
    inner: T,
    batch_size: usize,
}

impl<T> BatchSize<T>
where
    T: ResultStream + Unpin,
    T::Elem: Unpin,
{
    pub(super) fn new(inner: T, batch_size: usize) -> Self {
        Self { inner, batch_size }
    }

    pub fn batch_size(self, batch_size: usize) -> Self {
        Self {
            inner: self.inner,
            batch_size,
        }
    }

    pub async fn next(&mut self) -> Result<Vec<T::Elem>, T::Error> {
        self.inner.next(self.batch_size).await
    }

    pub async fn collect(mut self) -> Result<Vec<T::Elem>, T::Error> {
        let mut collection = Vec::new();
        loop {
            let batch = self.next().await?;
            if batch.is_empty() {
                break;
            }
            collection.extend(batch);
        }
        Ok(collection)
    }
}
