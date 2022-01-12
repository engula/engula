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

use aws_sdk_s3::SdkError;

use crate::Error;

impl<E, R> From<SdkError<E, R>> for Error
where
    R: std::fmt::Debug + 'static,
    E: std::error::Error + 'static,
{
    fn from(e: SdkError<E, R>) -> Self {
        Self::Unknown(Box::new(e))
    }
}
