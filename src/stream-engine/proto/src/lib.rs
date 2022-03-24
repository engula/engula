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

#![allow(clippy::all)]

mod cast;

pub mod master {
    pub use v1::*;

    pub mod v1 {
        tonic::include_proto!("streamengine.master.v1");
    }
}

pub use master::*;

pub mod store {
    pub use v1::*;

    pub mod meta {
        pub use v1::*;

        pub mod v1 {
            tonic::include_proto!("streamengine.store.meta.v1");
        }
    }

    pub mod manifest {
        pub use v1::*;

        pub mod v1 {
            tonic::include_proto!("streamengine.store.manifest.v1");
        }
    }

    pub mod v1 {
        tonic::include_proto!("streamengine.store.v1");
    }
}

pub use store::{manifest, meta::v1::*, v1::*};
