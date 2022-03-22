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

use std::{
    collections::HashSet,
    fs::File,
    path::Path,
    sync::{Arc, Condvar, Mutex},
    thread::JoinHandle,
};

use futures::channel::oneshot;
use prost::Message;
use stream_engine_common::error::IoKindResult;
use stream_engine_proto::{Record, RecordGroup};

use super::{reader::Reader as LogReader, writer::Writer as LogWriter, LogFileManager};
use crate::{fs::layout, DbOption, IoResult, Result};

struct Request {
    sender: oneshot::Sender<IoKindResult<u64>>,
    /// A shutdown is delivered if record is None.
    record: Option<Record>,
}

struct ChannelCore {
    requests: Vec<Request>,
    waitting: bool,
}

#[derive(Clone)]
struct Channel {
    core: Arc<(Mutex<ChannelCore>, Condvar)>,
}

impl Channel {
    fn new() -> Self {
        Channel {
            core: Arc::new((
                Mutex::new(ChannelCore {
                    requests: Vec::new(),
                    waitting: false,
                }),
                Condvar::new(),
            )),
        }
    }

    fn take(&self) -> Vec<Request> {
        let mut core = self.core.0.lock().unwrap();
        while core.requests.is_empty() {
            core.waitting = true;
            core = self.core.1.wait(core).unwrap();
        }
        std::mem::take(&mut core.requests)
    }

    fn append(&self, record: Record) -> oneshot::Receiver<IoKindResult<u64>> {
        let (sender, receiver) = oneshot::channel();
        let mut core = self.core.0.lock().unwrap();
        core.requests.push(Request {
            sender,
            record: Some(record),
        });
        if core.waitting {
            core.waitting = false;
            self.core.1.notify_one();
        }
        receiver
    }

    fn shutdown(&self) {
        let (sender, _) = oneshot::channel();
        let mut core = self.core.0.lock().unwrap();
        core.requests.push(Request {
            sender,
            record: None,
        });
        if core.waitting {
            core.waitting = false;
            self.core.1.notify_one();
        }
    }
}

#[derive(Clone)]
pub struct LogEngine {
    channel: Channel,
    log_file_manager: LogFileManager,
    core: Arc<Mutex<LogEngineCore>>,
}

impl LogEngine {
    pub fn recover<P: AsRef<Path>, F>(
        base_dir: P,
        mut log_numbers: Vec<u64>,
        log_file_mgr: LogFileManager,
        reader: &mut F,
    ) -> Result<LogEngine>
    where
        F: FnMut(u64, Record) -> Result<()>,
    {
        let mut last_file_info = None;
        log_numbers.sort_unstable();
        for log_number in log_numbers {
            let (next_record_offset, refer_streams) =
                recover_log_file(&base_dir, log_number, reader)?;
            last_file_info = Some((log_number, next_record_offset));
            log_file_mgr.delegate(log_number, refer_streams);
        }

        let mut writer = None;
        let opt = log_file_mgr.option();
        if let Some((log_number, initial_offset)) = last_file_info {
            if initial_offset < opt.log.log_file_size as u64 {
                let file = File::options()
                    .write(true)
                    .open(layout::log(&base_dir, log_number))?;
                writer = Some(LogWriter::new(
                    file,
                    log_number,
                    initial_offset as usize,
                    opt.log.log_file_size,
                )?);
            }
        }

        let channel = Channel::new();
        let mut log_worker = LogWorker::new(channel.clone(), writer, log_file_mgr.clone())?;
        let worker_handle = std::thread::spawn(move || log_worker.run());

        Ok(LogEngine {
            channel,
            log_file_manager: log_file_mgr,
            core: Arc::new(Mutex::new(LogEngineCore {
                worker_handle: Some(worker_handle),
            })),
        })
    }

    #[inline(always)]
    pub fn log_file_manager(&self) -> LogFileManager {
        self.log_file_manager.clone()
    }

    pub fn add_record(&self, record: Record) -> oneshot::Receiver<IoKindResult<u64>> {
        self.channel.append(record)
    }
}

impl Drop for LogEngine {
    fn drop(&mut self) {
        let mut core = self.core.lock().unwrap();
        self.channel.shutdown();
        if let Some(handle) = core.worker_handle.take() {
            handle.join().unwrap_or_default();
        }
    }
}

struct LogEngineCore {
    worker_handle: Option<JoinHandle<()>>,
}

struct LogWorker {
    opt: Arc<DbOption>,
    channel: Channel,
    writer: LogWriter,
    log_file_mgr: LogFileManager,

    refer_streams: HashSet<u64>,
    grouped_requests: Vec<oneshot::Sender<IoKindResult<u64>>>,
}

impl LogWorker {
    fn new(
        channel: Channel,
        writer: Option<LogWriter>,
        log_file_mgr: LogFileManager,
    ) -> Result<Self> {
        let opt = log_file_mgr.option();
        let writer = match writer {
            Some(w) => w,
            None => {
                let (log_number, new_log_file) = log_file_mgr.allocate_file()?;
                LogWriter::new(new_log_file, log_number, 0, opt.log.log_file_size)?
            }
        };

        Ok(LogWorker {
            opt,
            channel,
            writer,
            log_file_mgr,
            refer_streams: HashSet::new(),
            grouped_requests: Vec::new(),
        })
    }

    fn run(&mut self) {
        let mut shutdown = false;
        while !shutdown {
            let mut requests = self.channel.take();
            while !requests.is_empty() {
                let mut size = 0;
                let drained = requests.drain_filter(|req| {
                    size += req
                        .record
                        .as_ref()
                        .map(|r| r.encoded_len())
                        .unwrap_or_default();
                    size < 128 * 1024
                });
                let mut record_group = RecordGroup {
                    records: Vec::default(),
                };

                let mut refer_streams = HashSet::new();
                for req in drained {
                    if let Some(record) = req.record {
                        refer_streams.insert(record.stream_id);
                        record_group.records.push(record);
                        self.grouped_requests.push(req.sender);
                    } else {
                        shutdown = true;
                    }
                }

                if record_group.records.is_empty() {
                    continue;
                }

                if let Err(err) = self.submit_requests(record_group) {
                    self.notify_grouped_requests(Err(err.kind()));
                    continue;
                }

                self.refer_streams.extend(refer_streams.into_iter());
                self.notify_grouped_requests(Ok(self.writer.log_number()));
            }
        }
    }

    fn submit_requests(&mut self, request_group: RecordGroup) -> IoResult<()> {
        let content = request_group.encode_to_vec();
        if self.writer.avail_space() < content.len() {
            self.switch_writer()?;
        }
        self.writer.add_record(&content)?;

        if self.opt.log.sync_data {
            self.writer.flush()?;
        }

        Ok(())
    }

    fn switch_writer(&mut self) -> IoResult<()> {
        self.writer.fill_entire_avail_space()?;
        self.writer.flush()?;
        self.log_file_mgr.delegate(
            self.writer.log_number(),
            std::mem::take(&mut self.refer_streams),
        );

        let (log_number, new_log_file) = self.log_file_mgr.allocate_file()?;
        self.writer = LogWriter::new(new_log_file, log_number, 0, self.opt.log.log_file_size)?;
        Ok(())
    }

    fn notify_grouped_requests(&mut self, result: IoKindResult<u64>) {
        for sender in std::mem::take(&mut self.grouped_requests) {
            sender.send(result).unwrap_or_default();
        }
    }
}

/// Recovers log file, returns the next record offset and the referenced
/// streams of the specifies log file.
fn recover_log_file<P: AsRef<Path>, F>(
    base_dir: P,
    log_number: u64,
    callback: &mut F,
) -> Result<(u64, HashSet<u64>)>
where
    F: FnMut(u64, Record) -> Result<()>,
{
    let file = File::open(layout::log(base_dir, log_number))?;
    let mut reader = LogReader::new(file, log_number, true)?;
    let mut refer_streams = HashSet::new();
    while let Some(record) = reader.read_record()? {
        let record_group: RecordGroup = Message::decode(record.as_slice())?;
        for record in record_group.records {
            let stream_id = record.stream_id;
            callback(log_number, record)?;
            refer_streams.insert(stream_id);
        }
    }
    Ok((reader.next_record_offset() as u64, refer_streams))
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use stream_engine_proto::{Entry, EntryType, Record};

    use super::*;

    fn new_tempdir() -> Result<PathBuf> {
        let dir = tempfile::tempdir()?.path().to_owned();
        std::fs::create_dir_all(&dir)?;
        Ok(dir)
    }

    #[tokio::test]
    async fn log_engine_recover() -> Result<()> {
        let mut opt = DbOption::default();
        opt.log.log_file_size = 1024 * 1024 * 32;
        let opt = Arc::new(opt);
        let dir = new_tempdir()?;
        let log_file_mgr = LogFileManager::new(&dir, 100, opt.clone());
        let log_engine = LogEngine::recover(&dir, vec![], log_file_mgr, &mut |_, _| Ok(()))?;

        let mut expect_contents = vec![];
        for size in 0..1024 {
            let content = vec![size as u8 % u8::MAX; size];
            let record = Record {
                stream_id: 1,
                epoch: 1,
                writer_epoch: Some(1),
                acked_seq: None,
                first_index: Some(1),
                entries: vec![Entry {
                    entry_type: EntryType::Event as i32,
                    epoch: 1,
                    event: content.clone(),
                }],
            };
            log_engine.add_record(record.clone()).await??;
            expect_contents.push(record);
        }

        drop(log_engine);

        let mut read_contents = vec![];
        let log_file_mgr = LogFileManager::new(&dir, 100, opt.clone());
        let reader = &mut |_, record| {
            read_contents.push(record);
            Ok(())
        };
        LogEngine::recover(&dir, vec![100], log_file_mgr, reader)?;

        assert_eq!(expect_contents.len(), read_contents.len());
        for i in 0..expect_contents.len() {
            assert_eq!(expect_contents[i], read_contents[i], "case {}", i);
        }

        Ok(())
    }
}
