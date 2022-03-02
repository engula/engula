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

use std::{fmt::Display, ops::Range};

use crate::{Entry, Sequence};

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub(crate) struct Learn {
    pub target: String,
    pub seg_epoch: u32,
    pub writer_epoch: u32,
    pub start_index: u32,
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub(crate) struct Mutate {
    pub target: String,
    pub seg_epoch: u32,
    pub writer_epoch: u32,
    pub kind: MutKind,
}

#[derive(Debug, Clone)]
pub(crate) enum MutKind {
    Write(Write),
    Seal,
}

#[derive(Derivative, Clone)]
#[derivative(Debug)]
pub(crate) struct Write {
    pub acked_seq: Sequence,
    pub range: Range<u32>,
    pub bytes: usize,
    #[derivative(Debug = "ignore")]
    pub entries: Vec<Entry>,
}

#[derive(Derivative, Clone)]
#[derivative(Debug)]
pub(crate) struct Learned {
    // The end is reached if entries is empty.
    pub entries: Vec<(u32, Entry)>,
}

/// Restored is used to notify the worker to send a message to the master to
/// seal the corresponding segment.
#[allow(unused)]
#[derive(Clone, Debug)]
pub(crate) struct Restored {
    pub segment_epoch: u32,
    pub writer_epoch: u32,
}

#[derive(Derivative, Clone)]
#[derivative(Debug)]
#[allow(unused)]
pub(crate) enum MsgDetail {
    Received {
        matched_index: u32,
        acked_index: u32,
    },
    Recovered,
    Rejected,
    Timeout {
        range: Option<Range<u32>>,
        bytes: usize,
    },
    Sealed {
        acked_index: u32,
    },
    Learned(Learned),
}

impl Display for MsgDetail {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let desc = match self {
            MsgDetail::Received { .. } => "RECEIVED",
            MsgDetail::Recovered => "RECOVERED",
            MsgDetail::Rejected => "REJECTED",
            MsgDetail::Timeout { .. } => "TIMEOUT",
            MsgDetail::Sealed { .. } => "SEALED",
            MsgDetail::Learned(_) => "LEARNED",
        };
        write!(f, "{}", desc)
    }
}

/// An abstraction of data communication between `StreamStateMachine` and
/// journal servers.
#[derive(Debug, Clone)]
pub(crate) struct Message {
    pub target: String,
    pub segment_epoch: u32,
    pub writer_epoch: u32,
    pub detail: MsgDetail,
}

impl Message {
    #[inline(always)]
    pub fn master_timeout(segment_epoch: u32, writer_epoch: u32) -> Self {
        Self::store_timeout("<MASTER>".into(), segment_epoch, writer_epoch)
    }

    #[inline(always)]
    pub fn store_timeout(target: String, segment_epoch: u32, writer_epoch: u32) -> Self {
        Self::write_timeout(target, segment_epoch, writer_epoch, None, 0)
    }

    #[inline(always)]
    pub fn write_timeout(
        target: String,
        segment_epoch: u32,
        writer_epoch: u32,
        range: Option<Range<u32>>,
        bytes: usize,
    ) -> Self {
        Message {
            target,
            segment_epoch,
            writer_epoch,
            detail: MsgDetail::Timeout { range, bytes },
        }
    }

    #[inline(always)]
    pub fn sealed(target: String, segment_epoch: u32, writer_epoch: u32, acked_index: u32) -> Self {
        Message {
            target,
            segment_epoch,
            writer_epoch,
            detail: MsgDetail::Sealed { acked_index },
        }
    }

    #[inline(always)]
    pub fn received(
        target: String,
        segment_epoch: u32,
        writer_epoch: u32,
        matched_index: u32,
        acked_index: u32,
    ) -> Self {
        Message {
            target,
            segment_epoch,
            writer_epoch,
            detail: MsgDetail::Received {
                matched_index,
                acked_index,
            },
        }
    }

    #[inline(always)]
    pub fn recovered(segment_epoch: u32, writer_epoch: u32) -> Self {
        Message {
            target: "<MASTER>".into(),
            segment_epoch,
            writer_epoch,
            detail: MsgDetail::Recovered,
        }
    }

    #[inline(always)]
    pub fn learned(
        target: String,
        segment_epoch: u32,
        writer_epoch: u32,
        learned: Learned,
    ) -> Self {
        Message {
            target,
            segment_epoch,
            writer_epoch,
            detail: MsgDetail::Learned(learned),
        }
    }
}
