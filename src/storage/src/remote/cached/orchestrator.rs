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

use engula_futures::stream::batch::ResultStream;

use crate::{async_trait, Error, Result, Storage};

#[async_trait]
pub trait Orchestrator: Send + Sync + 'static {
    type Instance: Storage + Unpin;
    type InstanceLister: ResultStream<Elem = Self::Instance, Error = Error> + Unpin;

    async fn list_instances(&self) -> Result<Self::InstanceLister>;

    async fn provision_instance(&self) -> Result<Self::Instance>;

    async fn deprovision_instance(&self, instance: Self::Instance) -> Result<()>;
}
