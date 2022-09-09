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

use std::time::{Duration, Instant};

use prometheus::proto::Metric;

use super::metrics::*;

pub(super) struct ReportContext {
    instant: Instant,
    get_success: Metric,
    get_failure: Metric,
    put_success: Metric,
    put_failure: Metric,
}

struct Summary {
    interval: Duration,
    get_success: Histogram,
    get_failure: Histogram,
    put_success: Histogram,
    put_failure: Histogram,
}

struct Histogram {
    count: usize,
    avg: usize,
    p99: usize,
    p999: usize,
    p9999: usize,
    min: usize,
    max: usize,
}

impl From<prometheus::proto::Metric> for Histogram {
    fn from(m: prometheus::proto::Metric) -> Self {
        let h = m.get_histogram();
        let us = |v: f64| -> usize { (v * 1000. * 1000.) as usize };
        let value = |percent: f64| -> usize {
            let cnt = (percent * h.get_sample_count() as f64) + 0.5;
            h.get_bucket()
                .iter()
                .find(|b| b.get_cumulative_count() as f64 >= cnt)
                .map(|b| us(b.get_upper_bound()))
                .unwrap_or_default()
        };

        let max = h
            .get_bucket()
            .iter()
            .find(|v| v.get_cumulative_count() == h.get_sample_count())
            .map(|v| us(v.get_upper_bound()))
            .unwrap_or_default();

        let min = h
            .get_bucket()
            .iter()
            .take_while(|v| v.get_cumulative_count() == 0)
            .map(|v| us(v.get_upper_bound()))
            .last()
            .unwrap_or_default();

        let avg = if h.get_sample_count() == 0 {
            0
        } else {
            us(h.get_sample_sum() / h.get_sample_count() as f64)
        };

        Histogram {
            count: h.get_sample_count() as usize,
            avg,
            p99: value(0.99),
            p999: value(0.999),
            p9999: value(0.9999),
            min,
            max,
        }
    }
}

impl ReportContext {
    pub fn default() -> Self {
        use prometheus::core::Metric;
        let get_success = GET_SUCCESS_REQUEST_DURATION_SECONDS.metric();
        let get_failure = GET_FAILURE_REQUEST_DURATION_SECONDS.metric();
        let put_success = PUT_SUCCESS_REQUEST_DURATION_SECONDS.metric();
        let put_failure = PUT_FAILURE_REQUEST_DURATION_SECONDS.metric();
        ReportContext {
            instant: Instant::now(),
            get_success,
            get_failure,
            put_success,
            put_failure,
        }
    }
}

fn diff(current: &ReportContext, earlier: &ReportContext) -> Summary {
    Summary {
        interval: current.instant.saturating_duration_since(earlier.instant),
        get_success: Histogram::from(histogram_diff(&current.get_success, &earlier.get_success)),
        get_failure: Histogram::from(histogram_diff(&current.get_failure, &earlier.get_failure)),
        put_success: Histogram::from(histogram_diff(&current.put_success, &earlier.put_success)),
        put_failure: Histogram::from(histogram_diff(&current.put_failure, &earlier.put_failure)),
    }
}

fn histogram_diff(
    current: &prometheus::proto::Metric,
    earlier: &prometheus::proto::Metric,
) -> prometheus::proto::Metric {
    let mut m = prometheus::proto::Metric::default();
    let mut h = prometheus::proto::Histogram::default();
    let mut sum =
        current.get_histogram().get_sample_sum() - earlier.get_histogram().get_sample_sum();
    if sum < 0.0 {
        sum = 0.0;
    }
    h.set_sample_sum(sum);
    h.set_sample_count(
        current
            .get_histogram()
            .get_sample_count()
            .saturating_sub(earlier.get_histogram().get_sample_count()),
    );
    h.mut_bucket().extend(
        current
            .get_histogram()
            .get_bucket()
            .iter()
            .zip(earlier.get_histogram().get_bucket().iter())
            .map(|(b1, b2)| {
                let cumulative_count = b1.get_cumulative_count() - b2.get_cumulative_count();
                let mut b = prometheus::proto::Bucket::default();
                b.set_cumulative_count(cumulative_count);
                b.set_upper_bound(b1.get_upper_bound());
                b
            }),
    );

    m.set_histogram(h);
    m
}

pub(super) fn display(earlier_ctx: &mut ReportContext) {
    let mut current_ctx = ReportContext::default();
    let summary = diff(&current_ctx, earlier_ctx);

    display_histogram(
        "GET",
        &summary.get_success,
        summary.interval,
        current_ctx.get_success.get_histogram().get_sample_count(),
    );
    display_histogram(
        "PUT",
        &summary.put_success,
        summary.interval,
        current_ctx.put_success.get_histogram().get_sample_count(),
    );
    display_histogram(
        "GET_ERROR",
        &summary.get_failure,
        summary.interval,
        current_ctx.get_failure.get_histogram().get_sample_count(),
    );
    display_histogram(
        "PUT_ERROR",
        &summary.put_failure,
        summary.interval,
        current_ctx.put_failure.get_histogram().get_sample_count(),
    );
    std::mem::swap(earlier_ctx, &mut current_ctx);
}

fn display_histogram(name: &str, h: &Histogram, interval: Duration, count: u64) {
    if h.count == 0 {
        return;
    }

    let seconds = interval.as_secs_f64();
    let qps = (h.count as f64) / seconds;

    println!("{name} - Takes(s): {:.1}, Count: {}, OPS: {:.1}, Avg(us): {}, Min(us): {}, Max(us): {}, 99th(us): {}, 99.9th(us): {}, 99.99th(us): {}",
        seconds, count, qps, h.avg, h.min, h.max, h.p99, h.p999, h.p9999);
}
