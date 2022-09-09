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
use lazy_static::lazy_static;
use paste::paste;
use prometheus::*;

macro_rules! request_total {
    ($name: ident) => {
        paste! {
            lazy_static! {
                pub static ref [<$name:upper _REQUEST_TOTAL>]: IntCounter =
                        IntCounter::new(
                            concat!(stringify!($name), "_request_total"),
                            concat!("The total ", stringify!($name), " requests")).unwrap();
                pub static ref [<$name:upper _SUCCESS_REQUEST_TOTAL>]: IntCounter =
                        IntCounter::new(
                            concat!(stringify!($name), "_success_request_total"),
                            concat!("The total success ", stringify!($name), " requests")).unwrap();
                pub static ref [<$name:upper _FAILURE_REQUEST_TOTAL>]: IntCounter =
                        IntCounter::new(
                            concat!(stringify!($name), "_failure_request_total"),
                            concat!("The total failure ", stringify!($name), " requests")).unwrap();
                pub static ref [<$name:upper _SUCCESS_REQUEST_DURATION_SECONDS>]: Histogram =
                        register_histogram!(
                            concat!(stringify!($name), "_success_request_duration_seconds"),
                            concat!("The intervals of success ", stringify!($name), " requests"),
                            exponential_buckets(0.00005, 1.8, 26).unwrap(),
                        )
                        .unwrap();
                pub static ref [<$name:upper _FAILURE_REQUEST_DURATION_SECONDS>]: Histogram =
                        register_histogram!(
                            concat!(stringify!($name), "_failure_request_duration_seconds"),
                            concat!("The intervals of failure ", stringify!($name), " requests"),
                            exponential_buckets(0.00005, 1.8, 26).unwrap(),
                        )
                        .unwrap();
            }
        }
    };
}

request_total!(put);
request_total!(get);
