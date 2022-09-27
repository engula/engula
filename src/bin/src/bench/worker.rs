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

use std::time::Instant;

use engula_client::Collection;
use rand::prelude::*;
use tracing::trace;

use super::{metrics::*, AppConfig};

pub struct Job {
    co: Collection,

    consumed: usize,
    num_op: usize,
    gen: Generator,
}

pub struct Generator {
    cfg: AppConfig,
    range: std::ops::Range<u64>,
    rng: SmallRng,
}

#[derive(Debug, Clone)]
pub enum NextOp {
    Put { key: Vec<u8>, value: Vec<u8> },
    Get { key: Vec<u8> },
}

impl Generator {
    pub fn new(seed: u64, cfg: AppConfig, range: std::ops::Range<u64>) -> Generator {
        Generator {
            cfg,
            range,
            rng: SmallRng::seed_from_u64(seed),
        }
    }

    pub fn next_op(&mut self) -> NextOp {
        let v = self.rng.gen_range(0..100) as f64 / 100.0;
        let key = self.next_key();
        if v <= self.cfg.data.write {
            let value = self.next_bytes(self.cfg.data.value.clone());
            NextOp::Put { key, value }
        } else {
            NextOp::Get { key }
        }
    }

    fn next_bytes(&mut self, range: std::ops::Range<usize>) -> Vec<u8> {
        const BYTES: &[u8; 62] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        let len = self.rng.gen_range(range);
        let mut buf = vec![0u8; len];
        self.rng.fill(buf.as_mut_slice());
        buf.iter_mut().for_each(|v| *v = BYTES[(*v % 62) as usize]);
        buf
    }

    fn next_key(&mut self) -> Vec<u8> {
        let index = self.rng.gen_range(self.range.clone());
        format!(
            "{}{index:0leading$}",
            self.cfg.key.prefix,
            leading = self.cfg.key.leading
        )
        .into_bytes()
    }
}

impl Job {
    pub fn new(co: Collection, seed: u64, num_op: usize, cfg: AppConfig) -> Job {
        let limited = cfg.data.limited;
        Job {
            co,
            consumed: 0,
            num_op,
            gen: Generator::new(seed, cfg, 0..limited),
        }
    }
}

impl Iterator for Job {
    type Item = NextOp;

    fn next(&mut self) -> Option<Self::Item> {
        if self.consumed >= self.num_op {
            None
        } else {
            self.consumed += 1;
            Some(self.gen.next_op())
        }
    }
}

pub async fn worker_main(_id: usize, mut job: Job) {
    let co = job.co.clone();
    for next_op in &mut job {
        execute(&co, next_op).await;
    }
}

async fn execute(co: &Collection, next_op: NextOp) {
    match next_op {
        NextOp::Get { key } => {
            get(co, key).await;
        }
        NextOp::Put { key, value } => {
            put(co, key, value).await;
        }
    }
}

async fn get(co: &Collection, key: Vec<u8>) {
    trace!("send get request");
    let start = Instant::now();
    match co.get(key).await {
        Ok(_) => {
            GET_SUCCESS_REQUEST_TOTAL.inc();
            GET_SUCCESS_REQUEST_DURATION_SECONDS.observe(saturating_elapsed_seconds(start));
        }
        Err(e) => {
            tracing::error!("get request {e:?}");
            GET_FAILURE_REQUEST_TOTAL.inc();
            GET_FAILURE_REQUEST_DURATION_SECONDS.observe(saturating_elapsed_seconds(start));
        }
    }
    GET_REQUEST_TOTAL.inc();
}

async fn put(co: &Collection, key: Vec<u8>, value: Vec<u8>) {
    trace!("send put request");
    let start = Instant::now();
    match co.put(key, value).await {
        Ok(_) => {
            PUT_SUCCESS_REQUEST_TOTAL.inc();
            PUT_SUCCESS_REQUEST_DURATION_SECONDS.observe(saturating_elapsed_seconds(start));
        }
        Err(_) => {
            PUT_FAILURE_REQUEST_TOTAL.inc();
            PUT_FAILURE_REQUEST_DURATION_SECONDS.observe(saturating_elapsed_seconds(start));
        }
    }
    PUT_REQUEST_TOTAL.inc();
}

#[inline]
fn saturating_elapsed_seconds(instant: Instant) -> f64 {
    let now = Instant::now();
    now.saturating_duration_since(instant).as_secs_f64()
}
