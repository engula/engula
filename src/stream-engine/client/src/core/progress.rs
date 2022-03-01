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

use std::{collections::VecDeque, ops::Range};

use crate::Sequence;

/// A mixin structure holds the fields used in congestion stage.
struct CongestMixin {
    /// The latest tick retransmit entries.
    tick: usize,

    /// The number of bytes send to the target but invalid due to timeout.
    invalid_bytes: usize,

    /// The number of bytes send to the target during the congestion.
    recoup_bytes: usize,

    retransmit_ranges: VecDeque<(Range<u32>, usize)>,
}

impl CongestMixin {
    fn new(tick: usize) -> Self {
        CongestMixin {
            tick,
            invalid_bytes: 0,
            recoup_bytes: 0,
            retransmit_ranges: VecDeque::default(),
        }
    }

    /// Handle received events, and return whether the congestion is finished.
    fn on_received(&mut self, received_bytes: usize) -> bool {
        self.recoup_bytes += received_bytes;
        self.recoup_bytes >= self.invalid_bytes * 20
    }

    fn on_timeout(&mut self, range: Range<u32>, bytes: usize) {
        self.invalid_bytes += bytes;
        self.retransmit_ranges.insert(
            self.retransmit_ranges
                .binary_search_by_key(&range.start, |(r, _)| r.start)
                .err()
                .expect("this range shouldn't exists in the `retransmit_ranges`"),
            (range, bytes),
        );
    }

    fn might_replicate(&mut self, next_index: u32, replicate_bytes: usize) -> bool {
        if let Some((Range { start, end }, bytes)) = self.retransmit_ranges.pop_front() {
            // There still exists some pending entries.
            debug_assert!(start < next_index, "Progress::next_chunk()",);
            debug_assert!(next_index <= end, "Progress::next_chunk()");
            if next_index < end {
                self.retransmit_ranges
                    .push_front((next_index..end, bytes.saturating_sub(replicate_bytes)));
            }
            return true;
        }
        false
    }
}

#[allow(dead_code)]
struct SlidingWindow {
    /// The number of bytes which already known the response. Includes timeout
    /// and received entries.
    resp_bytes: usize,
    /// The number of bytes already send to target.
    send_bytes: usize,
    /// The capacity bytes of sliding window.
    capacity: usize,
    /// The capacity bytes of sliding window, its the initial size.
    actual_capacity: usize,

    window: VecDeque<(u32, usize)>,
}

impl SlidingWindow {
    fn new(capacity: usize) -> Self {
        SlidingWindow {
            resp_bytes: 0,
            send_bytes: 0,
            capacity,
            actual_capacity: capacity,

            window: VecDeque::new(),
        }
    }

    /// Returns the available space for the sliding window.
    fn available(&self) -> usize {
        let consumed = self.send_bytes.saturating_sub(self.resp_bytes);
        self.capacity.saturating_sub(consumed)
    }

    /// Allocate space and record relevant data.
    fn replicate(&mut self, next_index: u32, bytes: usize) {
        self.window.push_back((next_index, bytes));
        self.send_bytes += bytes;
    }

    /// Release all matched indexes and return the consumed bytes.
    fn release(&mut self, matched_index: u32) -> usize {
        let consumed_bytes = self
            .window
            .drain(..self.upper_bound(matched_index))
            .map(|(_, b)| b)
            .sum::<usize>();
        self.resp_bytes += consumed_bytes;
        consumed_bytes
    }

    /// Freeze available space.
    fn freeze(&mut self) {
        self.capacity = self.send_bytes.saturating_sub(self.resp_bytes);
    }

    fn melt(&mut self) {
        self.capacity = self.actual_capacity;
    }

    /// Like replicate but don't record indexes.
    fn shrink(&mut self, bytes: usize) {
        self.send_bytes += bytes;
    }

    fn extend(&mut self, bytes: usize) {
        self.resp_bytes += bytes;
    }

    fn upper_bound(&self, target: u32) -> usize {
        for (i, (index, _)) in self.window.iter().enumerate() {
            if *index > target {
                return i;
            }
        }
        self.window.len()
    }
}

/// An abstraction for describing the state of a segment store, that receives
/// and persists entries.
pub(crate) struct Progress {
    epoch: u32,

    /// The term matched means that all previous entries have been persisted.
    ///
    /// The default value is zero, so any proposal's index should greater than
    /// zero.
    matched_index: u32,
    next_index: u32,
    acked_index: u32,

    /// Indicates the largest acked index send to the remote (perhaps not yet
    /// accepted).
    replicating_acked_index: u32,

    current_tick: usize,
    sliding_window: SlidingWindow,

    /// Records congestion and retransmission data structures.  The link is
    /// normally if it is `None`.
    congest: Option<Box<CongestMixin>>,
}

impl Progress {
    pub fn new(epoch: u32) -> Self {
        Progress {
            epoch,
            matched_index: 0,
            next_index: 1,
            acked_index: 0,
            replicating_acked_index: 0,

            // TODO(w41ter) config sliding window
            sliding_window: SlidingWindow::new(1024 * 1024 * 64),
            current_tick: 0,
            congest: None,
        }
    }

    #[inline(always)]
    pub fn on_tick(&mut self) {
        self.current_tick += 1;
    }

    #[inline(always)]
    pub fn matched_index(&self) -> u32 {
        self.matched_index
    }

    /// Return which chunk needs to replicate to the target.
    pub fn next_chunk(&mut self, next_index: u32) -> (Range<u32>, usize) {
        let avail = self.sliding_window.available();
        if let Some(congest) = &mut self.congest {
            if let Some((range, bytes)) = congest.retransmit_ranges.front() {
                // Since timeout also advance `resp_bytes`, the avail bandwidth should great
                // than the required. See `Progress::on_timeout` for details.
                //
                // Sometimes the bandwidth is always insufficient (for example, all messages
                // have timed out), in order to avoid replication interruption, each tick tries
                // to send some data.
                if *bytes < avail || congest.tick < self.current_tick {
                    // TODO(w41ter) combine tiny continuously ranges.
                    congest.tick = self.current_tick;
                    return (range.clone(), avail.max(*bytes));
                } else {
                    return (self.next_index..self.next_index, 0);
                }
            }
        }

        if avail == 0 {
            (self.next_index..self.next_index, 0)
        } else if next_index > self.next_index {
            (self.next_index..next_index, avail)
        } else {
            (next_index..next_index, avail)
        }
    }

    pub fn replicate(&mut self, next_index: u32, replicate_bytes: usize, replicating_index: u32) {
        if self.replicating_acked_index < replicating_index {
            self.replicating_acked_index = replicating_index;
        }

        if self
            .congest
            .as_mut()
            .map(|c| c.might_replicate(next_index, replicate_bytes))
            .unwrap_or_default()
        {
            self.sliding_window.shrink(replicate_bytes)
        } else {
            debug_assert!(
                self.next_index <= next_index,
                "local next_index {}, but receive {}",
                self.next_index,
                next_index
            );

            self.next_index = next_index;
            self.sliding_window.replicate(next_index, replicate_bytes);
        }
    }

    /// The target has received and persisted entries.
    pub fn on_received(&mut self, matched_index: u32, acked_index: u32) {
        debug_assert!(acked_index <= matched_index);

        if self.acked_index < acked_index {
            self.acked_index = acked_index;
        }

        if self.replicating_acked_index < acked_index {
            self.replicating_acked_index = acked_index;
        }

        if self.matched_index < matched_index {
            let consumed_bytes = self.sliding_window.release(matched_index);
            self.matched_index = matched_index;
            if self.next_index < self.matched_index {
                self.next_index = self.matched_index + 1;
            }

            if let Some(congest) = &mut self.congest {
                if congest.on_received(consumed_bytes) {
                    self.congest = None;
                    self.sliding_window.melt();
                }
            }
        }
    }

    /// The message sent to target has timed out and no further responses will
    /// be received.
    pub fn on_timeout(&mut self, range: std::ops::Range<u32>, bytes: usize) {
        if self.congest.is_none() {
            self.sliding_window.freeze();
            self.congest = Some(Box::new(CongestMixin::new(self.current_tick)));
        }

        // NOTE: the sliding window now becomes available, so the retransmission
        // requires more available space.
        self.sliding_window.extend(bytes);
        self.congest.as_mut().unwrap().on_timeout(range, bytes);

        // A simple implementation, so that there is no need to record the corresponding
        // acked index in the timeout message.
        self.replicating_acked_index = 0;
    }

    /// Return whether a target has been matched to the corresponding index.
    #[inline(always)]
    pub fn is_acked(&self, index: u32) -> bool {
        index <= self.acked_index
    }

    /// Return whether the acked index has been send to a target.
    #[inline(always)]
    pub fn is_replicating_acked_seq(&self, seq: Sequence) -> bool {
        seq.epoch < self.epoch
            || (seq.epoch == self.epoch && seq.index <= self.replicating_acked_index)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retransmit() {
        let mut progress = Progress::new(1);
        progress.replicate(100, 1024, 0);
        progress.replicate(200, 1024, 0);
        progress.replicate(300, 1024, 0);
        progress.replicate(400, 1024, 0);
        progress.replicate(500, 1024, 0);

        // Now its timeout and lost entries in [300, 400).
        progress.on_timeout(300..400, 1024);

        // 1. Only available after receive another entry.
        // not available
        let (Range { start, end }, bytes) = progress.next_chunk(600);
        assert_eq!(start, end);
        assert_eq!(start, 500);
        assert_eq!(bytes, 0);

        // 2. retransmit first.
        progress.on_received(100, 0);
        let (Range { start, end }, bytes) = progress.next_chunk(600);
        assert_eq!(start, 300);
        assert_eq!(end, 400);
        progress.replicate(end, bytes, 0);

        // not available
        let (Range { start, end }, bytes) = progress.next_chunk(600);
        assert_eq!(start, end);
        assert_eq!(start, 500);
        assert_eq!(bytes, 0);

        // 3. try replicate normal entries.
        progress.on_received(200, 0);
        let (Range { start, end }, _) = progress.next_chunk(600);
        assert_eq!(start, 500);
        assert_eq!(end, 600);
    }

    #[test]
    fn deadlock_but_advance_by_tick() {
        let mut progress = Progress::new(1);
        progress.replicate(100, 1024, 0);
        progress.on_timeout(1..100, 1024);

        // not available
        let (Range { start, end }, bytes) = progress.next_chunk(600);
        assert_eq!(start, end);
        assert_eq!(start, 100);
        assert_eq!(bytes, 0);

        progress.on_tick();
        let (Range { start, end }, _) = progress.next_chunk(600);
        assert_eq!(start, 1);
        assert_eq!(end, 100);
    }

    #[test]
    fn timeout_reset_replicating_acked_index() {
        let mut progress = Progress::new(1);
        progress.replicate(100, 1024, 100);
        assert!(progress.is_replicating_acked_seq(Sequence::new(1, 100)));
        progress.on_timeout(1..100, 1024);
        assert!(!progress.is_replicating_acked_seq(Sequence::new(1, 100)));
    }

    #[test]
    fn next_index_always_great_than_matched_index() {
        let mut progress = Progress::new(1);
        progress.on_received(100, 0);
        assert!(progress.matched_index < progress.next_index);
    }
}
