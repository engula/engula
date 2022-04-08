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

#![feature(custom_test_frameworks)]
#![test_runner(criterion::runner)]

use bytes::Bytes;
use criterion::{Criterion, Throughput};
use criterion_macro::criterion;
use engula_engine::Db;
use rand::{distributions::Alphanumeric, Rng};

#[criterion]
fn db_get(c: &mut Criterion) {
    let bench_size = 10000;

    let db = Db::default();
    let mut keys = vec![];

    for _ in 0..bench_size {
        let key: String = rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(50)
            .map(char::from)
            .collect();
        let value: String = rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(50)
            .map(char::from)
            .collect();
        keys.push(Bytes::from(key.clone()));
        db.set(Bytes::from(key), Bytes::from(value));
    }

    let mut rng = rand::thread_rng();
    let mut group = c.benchmark_group("db_get_group");
    group.throughput(Throughput::Elements(bench_size));
    group.bench_function("db_get", |b| {
        b.iter(|| {
            for _ in 0..bench_size {
                let key = keys.get(rng.gen_range(0..keys.len())).unwrap();
                db.get(key);
            }
        });
    });
    group.finish();
}
