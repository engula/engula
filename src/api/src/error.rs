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

use super::server::v1::*;

impl GroupResponse {
    #[inline]
    pub fn new(response: group_response_union::Response) -> Self {
        GroupResponse {
            response: Some(GroupResponseUnion {
                response: Some(response),
            }),
            error: None,
        }
    }

    #[inline]
    pub fn with_error(resp: group_response_union::Response, error: Error) -> Self {
        GroupResponse {
            response: Some(GroupResponseUnion {
                response: Some(resp),
            }),
            error: Some(error),
        }
    }

    #[inline]
    pub fn error(error: Error) -> Self {
        GroupResponse {
            response: None,
            error: Some(error),
        }
    }
}

impl ErrorDetailUnion {
    #[inline]
    pub fn is_retryable(&self) -> bool {
        use error_detail_union::Value;
        matches!(
            &self.value,
            Some(
                Value::GroupNotFound(_)
                    | Value::NotLeader(_)
                    | Value::NotMatch(_)
                    | Value::NotRoot(_)
                    | Value::ServerIsBusy(_),
            )
        )
    }
}

impl ErrorDetail {
    #[inline]
    pub fn is_retryable(&self) -> bool {
        self.detail
            .as_ref()
            .map(ErrorDetailUnion::is_retryable)
            .unwrap_or_default()
    }
}

impl ErrorDetail {
    #[inline]
    pub fn new(value: error_detail_union::Value) -> Self {
        ErrorDetail {
            detail: Some(ErrorDetailUnion { value: Some(value) }),
            ..Default::default()
        }
    }

    #[inline]
    pub fn with_message(value: error_detail_union::Value, message: String) -> Self {
        ErrorDetail {
            detail: Some(ErrorDetailUnion { value: Some(value) }),
            message,
        }
    }

    #[inline]
    pub fn not_leader(value: NotLeader) -> Self {
        Self::new(error_detail_union::Value::NotLeader(value))
    }

    #[inline]
    pub fn server_is_busy(value: ServerIsBusy) -> Self {
        Self::new(error_detail_union::Value::ServerIsBusy(value))
    }

    #[inline]
    pub fn not_match(value: EpochNotMatch) -> Self {
        Self::new(error_detail_union::Value::NotMatch(value))
    }

    #[inline]
    pub fn group_not_found(value: GroupNotFound) -> Self {
        Self::new(error_detail_union::Value::GroupNotFound(value))
    }

    #[inline]
    pub fn status(code: i32, msg: impl Into<String>) -> Self {
        Self::with_message(error_detail_union::Value::StatusCode(code), msg.into())
    }
}

impl Error {
    #[inline]
    pub fn not_leader(group_id: u64, leader: Option<ReplicaDesc>) -> Self {
        Self::with_detail_value(error_detail_union::Value::NotLeader(NotLeader {
            group_id,
            leader,
        }))
    }

    #[inline]
    pub fn not_root_leader(root: RootDesc, leader: Option<ReplicaDesc>) -> Self {
        Self::with_detail_value(error_detail_union::Value::NotRoot(NotRoot {
            root: Some(root),
            leader,
        }))
    }

    #[inline]
    pub fn server_is_busy() -> Self {
        Self::with_detail_value(error_detail_union::Value::ServerIsBusy(ServerIsBusy {}))
    }

    #[inline]
    pub fn not_match(desc: GroupDesc) -> Self {
        Self::with_detail_value(error_detail_union::Value::NotMatch(EpochNotMatch {
            descriptor: Some(desc),
        }))
    }

    #[inline]
    pub fn group_not_found(group_id: u64) -> Self {
        Self::with_detail_value(error_detail_union::Value::GroupNotFound(GroupNotFound {
            group_id,
        }))
    }

    #[inline]
    pub fn status(code: i32, msg: impl Into<String>) -> Self {
        Error {
            details: vec![ErrorDetail::status(code, msg)],
        }
    }

    #[inline]
    pub fn with_detail_value(value: error_detail_union::Value) -> Self {
        Error {
            details: vec![ErrorDetail::new(value)],
        }
    }
}
