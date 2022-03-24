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
    cmp::Ordering,
    collections::{HashMap, HashSet},
    ffi::OsStr,
    io::{Seek, SeekFrom},
    os::unix::prelude::OsStrExt,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use prost::Message;
use stream_engine_proto::manifest::{self, RecycleLog, StreamMeta};
use tracing::error;

use crate::{
    fs::{layout, FileExt},
    log::{reader::Reader as LogReader, writer::Writer as LogWriter},
    sync::atomic_arc::AtomicArcPtr,
    Error, Result,
};

const MAX_DESCRIPTOR_FILE_SIZE: usize = 4 * 1024 * 1024;

#[derive(Default)]
pub struct VersionEdit {
    raw_edit: manifest::VersionEdit,

    /// A shared pointer point to the next VersionEdit.
    next_edit: AtomicArcPtr<VersionEdit>,
}

impl VersionEdit {
    fn encode_to_vec(&self) -> Vec<u8> {
        self.raw_edit.encode_to_vec()
    }
}

#[derive(Clone)]
pub struct LogNumberRecord {
    /// The min useful log number. All log files with small log number would be
    /// released safety.
    ///
    /// DEFAULT: [`MIN_AVAIL_LOG_NUMBER`]
    pub min_log_number: u64,
    pub recycled_log_numbers: HashSet<u64>,
}

impl Default for LogNumberRecord {
    fn default() -> Self {
        LogNumberRecord {
            min_log_number: MIN_AVAIL_LOG_NUMBER,
            recycled_log_numbers: HashSet::default(),
        }
    }
}

impl LogNumberRecord {
    pub fn is_log_recycled(&self, log_number: u64) -> bool {
        log_number < self.min_log_number || self.recycled_log_numbers.contains(&log_number)
    }
}

#[derive(Clone)]
pub struct StreamVersion {
    pub stream_id: u64,
    pub log_number_record: LogNumberRecord,

    pub stream_meta: manifest::StreamMeta,

    next_edit: AtomicArcPtr<VersionEdit>,
}

impl StreamVersion {
    #[allow(dead_code)]
    pub fn new(stream_id: u64) -> Self {
        let stream_meta = StreamMeta {
            stream_id,
            acked_seq: 0,
            initial_seq: 0,
            replicas: vec![],
        };
        StreamVersion {
            stream_id,
            stream_meta,
            log_number_record: Default::default(),
            next_edit: Default::default(),
        }
    }

    #[allow(dead_code)]
    #[inline(always)]
    pub fn is_log_recycled(&self, log_number: u64) -> bool {
        self.log_number_record.is_log_recycled(log_number)
    }

    #[inline(always)]
    pub fn try_apply_edits(&mut self) -> bool {
        VersionBuilder::try_apply_edits_about_stream(self)
    }
}

pub const MIN_AVAIL_LOG_NUMBER: u64 = 1;

#[derive(Clone, Default)]
pub struct Version {
    pub log_number_record: LogNumberRecord,
    pub streams: HashMap<u64, manifest::StreamMeta>,

    next_edit: AtomicArcPtr<VersionEdit>,
}

impl Version {
    pub fn stream_version(&self, stream_id: u64) -> StreamVersion {
        let stream_meta = if let Some(stream_meta) = self.streams.get(&stream_id).cloned() {
            stream_meta
        } else {
            StreamMeta {
                stream_id,
                acked_seq: 0,
                initial_seq: 0,
                replicas: vec![],
            }
        };
        StreamVersion {
            stream_id,
            log_number_record: self.log_number_record.clone(),
            stream_meta,
            next_edit: self.next_edit.clone(),
        }
    }

    #[allow(dead_code)]
    #[inline(always)]
    pub fn is_log_recycled(&self, log_number: u64) -> bool {
        self.log_number_record.is_log_recycled(log_number)
    }

    #[inline(always)]
    pub fn try_apply_edits(&mut self) -> bool {
        VersionBuilder::try_apply_edits(self)
    }

    fn install_edit(&mut self, mut edit: Box<VersionEdit>) {
        loop {
            self.try_apply_edits();
            match self.next_edit.compare_store(edit) {
                Ok(()) => return,
                Err(e) => edit = e,
            }
        }
    }

    fn snapshot(&self) -> manifest::VersionEdit {
        manifest::VersionEdit {
            streams: self.streams.values().cloned().collect(),
            min_log_number: if self.log_number_record.min_log_number == MIN_AVAIL_LOG_NUMBER {
                None
            } else {
                Some(self.log_number_record.min_log_number)
            },
            recycled_logs: self
                .log_number_record
                .recycled_log_numbers
                .iter()
                .map(|log_number| RecycleLog {
                    log_number: *log_number,
                    ..Default::default()
                })
                .collect(),
        }
    }
}

#[derive(Clone)]
pub(crate) struct VersionSet {
    core: Arc<Mutex<VersionSetCore>>,
}

struct VersionSetCore {
    base_dir: PathBuf,
    writer: LogWriter,

    // Recover from the maximum file.
    next_file_number: u64,
    manifest_number: u64,

    version: Version,
}

impl VersionSet {
    pub fn create<P: AsRef<Path>>(base_dir: P) -> Result<()> {
        let manifest_number = 1;
        let version = Version::default();
        create_new_manifest(base_dir, &version, manifest_number)?;
        Ok(())
    }

    pub fn recover<P: AsRef<Path>>(base_dir: P) -> Result<VersionSet> {
        let manifest = parse_current_file(&base_dir).unwrap();
        let manifest_number = match layout::parse_file_name(&manifest).unwrap() {
            layout::FileType::Manifest(number) => number,
            _ => return Err(Error::Corruption("Invalid MANIFEST file name".to_owned())),
        };

        let (initial_offset, version) = recover_manifest(&manifest).unwrap();
        let mut file = std::fs::File::options()
            .write(true)
            .open(&manifest)
            .unwrap();
        file.seek(SeekFrom::Start(initial_offset as u64)).unwrap();
        let writer = LogWriter::new(file, 0, initial_offset, MAX_DESCRIPTOR_FILE_SIZE).unwrap();

        Ok(VersionSet {
            core: Arc::new(Mutex::new(VersionSetCore {
                base_dir: base_dir.as_ref().to_owned(),
                writer,
                next_file_number: 0,
                manifest_number,
                version,
            })),
        })
    }

    #[inline]
    pub fn manifest_number(&self) -> u64 {
        self.core.lock().unwrap().manifest_number
    }

    pub fn current(&self) -> Version {
        self.core.lock().unwrap().version.clone()
    }

    pub fn set_next_file_number(&self, file_number: u64) {
        let mut core = self.core.lock().unwrap();
        debug_assert!(core.next_file_number < file_number);
        core.next_file_number = file_number;
    }

    /// Allocate a increasing file number.
    // pub fn alloc_file_number(&self) -> u64 {
    //     let mut core = self.core.lock().unwrap();
    //     let file_number = core.next_file_number;
    //     core.next_file_number += 1;
    //     file_number
    // }

    pub async fn truncate_stream(&self, stream_meta: StreamMeta) -> Result<()> {
        let mut core = self.core.lock().unwrap();
        // Ensure there no any edit would be added before this one is finished.
        core.version.try_apply_edits();

        let stream_id = stream_meta.stream_id;
        if let Some(former) = core.version.streams.get(&stream_id) {
            match former.initial_seq.cmp(&stream_meta.initial_seq) {
                Ordering::Greater => {
                    return Err(Error::Staled(format!(
                        "stream {} has been truncated to {}",
                        stream_id, former.initial_seq
                    )));
                }
                Ordering::Equal => {
                    return Ok(());
                }
                _ => {}
            }
            // TODO(walter) merge stream meta.
        }

        let version_edit = Box::new(VersionEdit {
            raw_edit: manifest::VersionEdit {
                streams: vec![stream_meta],
                ..Default::default()
            },
            ..Default::default()
        });
        core.log_and_apply(version_edit)
    }
}

impl VersionSetCore {
    fn log_and_apply(&mut self, version_edit: Box<VersionEdit>) -> Result<()> {
        let content = version_edit.encode_to_vec();
        if self.writer.avail_space() < content.len() {
            self.writer.fill_entire_avail_space()?;
            self.writer.flush()?;

            self.writer =
                create_new_manifest(&self.base_dir, &self.version, self.next_file_number)?;
            self.next_file_number += 1;
        }
        self.writer.add_record(&content)?;
        self.writer.flush()?;
        self.version.install_edit(version_edit);

        Ok(())
    }
}

fn write_snapshot(writer: &mut LogWriter, version: &Version) -> Result<()> {
    let snapshot = version.snapshot();
    let content = snapshot.encode_to_vec();
    writer.add_record(content.as_slice())?;
    writer.flush()?;
    Ok(())
}

fn create_new_manifest<P: AsRef<Path>>(
    base_dir: P,
    version: &Version,
    manifest_number: u64,
) -> Result<LogWriter> {
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .open(&layout::descriptor(&base_dir, manifest_number))?;
    file.preallocate(MAX_DESCRIPTOR_FILE_SIZE)?;
    let mut writer = LogWriter::new(file, 0, 0, MAX_DESCRIPTOR_FILE_SIZE)?;
    write_snapshot(&mut writer, version)?;
    switch_current_file(&base_dir, manifest_number)?;

    Ok(writer)
}

/// Read and parse CURRENT file, return the path of corresponding MANIFEST file.
fn parse_current_file<P: AsRef<Path>>(base_dir: P) -> Result<PathBuf> {
    let content = match std::fs::read_to_string(layout::current(&base_dir)) {
        Ok(content) => content,
        Err(err) => {
            error!("read CURRENT file: {:?}", err);
            return Err(err.into());
        }
    };

    let content = match content.as_bytes().strip_suffix(&[b'\n']) {
        Some(content) => content,
        None => {
            return Err(Error::Corruption(
                "CURRENT file does not end with newline".to_owned(),
            ));
        }
    };

    Ok(base_dir
        .as_ref()
        .join(Path::new(OsStr::from_bytes(content))))
}

// Update CURRENT file and point to the corresponding MANIFEST file.
fn switch_current_file<P: AsRef<Path>>(base_dir: P, manifest_number: u64) -> Result<()> {
    let tmp = layout::temp(&base_dir, manifest_number);
    let content = format!("{}\n", layout::manifest(manifest_number));
    std::fs::write(&tmp, content)?;
    std::fs::rename(&tmp, layout::current(&base_dir))?;
    std::fs::File::open(&base_dir)?.sync_all()?;
    Ok(())
}

/// Read and apply version edits, returns the finalized version and the next
/// record offset.
fn recover_manifest<P: AsRef<Path>>(manifest: P) -> Result<(usize, Version)> {
    let file = std::fs::File::open(manifest)?;
    let mut reader = LogReader::new(file, 0, true)?;
    let mut builder = VersionBuilder::default();
    // FIXME(walter) handle partial write or corruption?
    while let Some(content) = reader.read_record()? {
        let edit =
            manifest::VersionEdit::decode(content.as_slice()).expect("corrupted version edit");
        builder.apply(edit);
    }
    let version = builder.finalize();
    Ok((reader.next_record_offset(), version))
}

#[derive(Default)]
pub(crate) struct VersionBuilder {
    version: Version,
}

impl VersionBuilder {
    pub fn apply(&mut self, edit: manifest::VersionEdit) {
        if !edit.streams.is_empty() {
            // This is a snapshot edit.
            self.version.streams = edit
                .streams
                .iter()
                .map(|s| (s.stream_id, s.clone()))
                .collect();
        }
        Self::apply_edit(&mut self.version, &edit);
    }

    #[inline(always)]
    pub fn finalize(self) -> Version {
        self.version
    }

    fn try_apply_edits(version: &mut Version) -> bool {
        // Do fast return, to avoid increasing reference count.
        if version.next_edit.try_deref().is_none() {
            return false;
        }

        let mut next_edit = version.next_edit.clone();
        while let Some(edit) = next_edit.try_deref() {
            Self::apply_edit(version, &edit.raw_edit);
            next_edit = edit.next_edit.clone();
            version.next_edit = next_edit.clone();
        }
        true
    }

    fn try_apply_edits_about_stream(version: &mut StreamVersion) -> bool {
        // Do fast return, to avoid increasing reference count.
        if version.next_edit.try_deref().is_none() {
            return false;
        }

        let mut next_edit = version.next_edit.clone();
        while let Some(edit) = next_edit.try_deref() {
            Self::apply_edit_about_stream(version, &edit.raw_edit);
            next_edit = edit.next_edit.clone();
            version.next_edit = next_edit.clone();
        }
        true
    }

    fn apply_edit(version: &mut Version, edit: &manifest::VersionEdit) {
        for recycle_log in &edit.recycled_logs {
            version
                .log_number_record
                .recycled_log_numbers
                .insert(recycle_log.log_number);
            for update in &recycle_log.updated_streams {
                let stream_meta = version
                    .streams
                    .entry(update.stream_id)
                    .or_insert_with(Default::default);
                Self::merge_stream(stream_meta, update);
            }
        }
        Self::advance_min_log_number(&mut version.log_number_record, edit);
    }

    fn apply_edit_about_stream(version: &mut StreamVersion, edit: &manifest::VersionEdit) {
        for recycle_log in &edit.recycled_logs {
            version
                .log_number_record
                .recycled_log_numbers
                .insert(recycle_log.log_number);
            for update in &recycle_log.updated_streams {
                if update.stream_id != version.stream_id {
                    continue;
                }
                Self::merge_stream(&mut version.stream_meta, update);
            }
        }
        Self::advance_min_log_number(&mut version.log_number_record, edit);
    }

    fn advance_min_log_number(record: &mut LogNumberRecord, edit: &manifest::VersionEdit) {
        if let Some(min_log_number) = edit.min_log_number {
            if record.min_log_number < min_log_number {
                record.min_log_number = min_log_number;
                record
                    .recycled_log_numbers
                    .drain_filter(|log_number| *log_number < record.min_log_number);
            }
        }
    }

    fn merge_stream(stream_meta: &mut manifest::StreamMeta, update: &manifest::StreamMeta) {
        let mut sealing_epochs = update
            .replicas
            .iter()
            .map(|r| (r.epoch, r.promised_epoch))
            .collect::<HashMap<_, _>>();
        for replica in &mut stream_meta.replicas {
            if let Some(sealing_epoch) = sealing_epochs.remove(&replica.epoch).flatten() {
                replica.promised_epoch = Some(sealing_epoch);
            }
        }
        stream_meta.initial_seq = update.initial_seq;
        stream_meta.acked_seq = update.acked_seq;
    }
}
