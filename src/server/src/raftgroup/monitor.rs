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
use std::mem::MaybeUninit;

use serde::Serialize;

#[derive(Clone, Default, Debug, Serialize)]
pub struct WorkerPerfContext {
    pub advance: AdvancePerfContext,
    pub wait: u64,
    pub compact_log: u64,
    pub wake: u64,
    pub consume_requests: u64,
    pub write: u64,
    pub finish: u64,

    pub num_writes: usize,
    pub num_requests: usize,
    pub num_proposal: usize,
    pub num_step_msg: usize,
    pub accumulated_bytes: usize,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct ApplierPerfContext {
    pub num_committed: usize,
    pub start_plug: u64,
    pub finish_plug: u64,
    pub response_proposals: u64,
}

#[derive(Clone, Default, Debug, Serialize)]
pub struct AdvancePerfContext {
    pub applier: ApplierPerfContext,
    pub take_ready: u64,
    pub send_message: u64,
}

#[inline]
pub(crate) fn record_perf_point(hold: &mut u64) {
    *hold = perf_point_micros();
}

#[inline]
pub(crate) fn perf_point_micros() -> u64 {
    if cfg!(target_os = "linux") {
        let mut t = MaybeUninit::uninit();
        unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, t.as_mut_ptr()) };
        let now: libc::timespec = unsafe { t.assume_init() };
        let micros = now.tv_nsec / 1000;
        (now.tv_sec * 1000 * 1000 + micros) as u64
    } else {
        0
    }
}
