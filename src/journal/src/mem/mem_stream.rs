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

use std::{collections::VecDeque, sync::Arc};

use futures::stream;
use tokio::sync::Mutex;

use crate::{async_trait, Error, JournalRecord, JournalStream, Result, SequenceNumber, Stream};

#[derive(Clone)]
pub struct MemStream {
    records: Arc<Mutex<VecDeque<JournalRecord>>>,
}

impl Default for MemStream {
    fn default() -> MemStream {
        MemStream {
            records: Arc::new(Mutex::new(VecDeque::new())),
        }
    }
}

#[async_trait]
impl JournalStream for MemStream {
    async fn read_records(&self, sn: SequenceNumber) -> Stream<Result<JournalRecord>> {
        let records = self.records.lock().await;
        let index = records.partition_point(|x| x.sn < sn);
        let iter = records
            .range(index..)
            .cloned()
            .map(Ok)
            .collect::<Vec<Result<JournalRecord>>>();
        Box::new(stream::iter(iter))
    }

    async fn append_record(&self, sn: SequenceNumber, data: Vec<u8>) -> Result<()> {
        let record = JournalRecord { sn, data };
        let mut records = self.records.lock().await;
        let last_sn = records.back().map(|x| x.sn).unwrap_or(0);
        if sn <= last_sn {
            return Err(Error::InvalidArgument(format!(
                "sequence number {} <= last sequence number {}",
                sn, last_sn
            )));
        }
        records.push_back(record);
        Ok(())
    }

    async fn release_records(&self, sn: SequenceNumber) -> Result<()> {
        let mut records = self.records.lock().await;
        let index = records.partition_point(|x| x.sn < sn);
        records.drain(..index);
        Ok(())
    }
}
