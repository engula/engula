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

mod kernel;

pub use self::{kernel::Kernel, mem::Kernel as MemKernel};

mod mem {
    use engula_journal::mem::Journal;
    use engula_storage::MemStorage;

    use crate::Result;

    pub type Kernel<T> = super::Kernel<Journal<T>, MemStorage>;

    impl<T> Kernel<T> {
        pub async fn open() -> Result<Self> {
            let journal = Journal::default();
            let storage = MemStorage::default();
            Self::init(journal, storage).await
        }
    }
}
