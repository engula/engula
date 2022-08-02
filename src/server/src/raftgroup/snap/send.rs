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
    ffi::OsStr,
    fs::File,
    io::Read,
    os::unix::ffi::OsStrExt,
    pin::Pin,
    task::{Context, Poll},
};

use tracing::debug;

use super::{SnapManager, SnapshotGuard};
use crate::{
    raftgroup::metrics::*,
    serverpb::v1::{snapshot_chunk, SnapshotChunk},
    Error, Result,
};

type SnapResult = std::result::Result<SnapshotChunk, tonic::Status>;

pub struct SnapshotChunkStream {
    info: SnapshotGuard,
    file: Option<File>,
    file_index: usize,
}

pub async fn send_snapshot(
    snap_mgr: &SnapManager,
    replica_id: u64,
    snapshot_id: Vec<u8>,
) -> Result<SnapshotChunkStream> {
    let snapshot_info = match snap_mgr.lock_snap(replica_id, &snapshot_id) {
        Some(snap_info) => snap_info,
        None => {
            return Err(Error::InvalidArgument("no such snapshot".to_string()));
        }
    };

    RAFTGROUP_SEND_SNAPSHOT_TOTAL.inc();
    Ok(SnapshotChunkStream::new(snapshot_info))
}

impl SnapshotChunkStream {
    fn new(info: SnapshotGuard) -> Self {
        SnapshotChunkStream {
            info,
            file: None,
            file_index: 0,
        }
    }

    fn next_chunk(&mut self) -> Option<SnapResult> {
        use std::{fs::OpenOptions, io::ErrorKind};

        match self.file.as_mut() {
            // Send snapshot file chunk.
            Some(file) => {
                let num_bytes = 32 * 1024;
                let mut num_read = 0;
                let mut chunk_data = vec![0; num_bytes];
                while num_read < num_bytes {
                    let n = match file.read(&mut chunk_data[num_read..]) {
                        Ok(n) => n,
                        Err(err) if err.kind() == ErrorKind::Interrupted => {
                            continue;
                        }
                        Err(err) => return Some(Err(err.into())),
                    };
                    num_read += n;
                    if n == 0 {
                        self.file = None;
                        self.file_index += 1;
                        break;
                    }
                }
                chunk_data.truncate(num_read);
                RAFTGROUP_SEND_SNAPSHOT_BYTES_TOTAL.inc_by(num_read as u64);
                let value = snapshot_chunk::Value::ChunkData(chunk_data);
                Some(Ok(SnapshotChunk { value: Some(value) }))
            }
            // Open new file and send file meta.
            None if self.file_index < self.info.meta.files.len() => {
                let file_meta = &self.info.meta.files[self.file_index];
                let path = self.info.base_dir.join(OsStr::from_bytes(&file_meta.name)); // Eg: `DATA/1.sst`.
                debug!(
                    "send file {} to remote, crc32 {}, size {}",
                    path.display(),
                    file_meta.crc32,
                    file_meta.size
                );
                match OpenOptions::new().read(true).open(&path) {
                    Ok(file) => self.file = Some(file),
                    Err(err) => return Some(Err(err.into())),
                }
                let value = snapshot_chunk::Value::File(file_meta.to_owned());
                Some(Ok(SnapshotChunk { value: Some(value) }))
            }
            // Send snapshot meta.
            None if self.file_index == self.info.meta.files.len() => {
                self.file_index += 1;
                let value = snapshot_chunk::Value::Meta(self.info.meta.clone());
                Some(Ok(SnapshotChunk { value: Some(value) }))
            }
            // All files and meta are send.
            None => None,
        }
    }
}

impl futures::Stream for SnapshotChunkStream {
    type Item = SnapResult;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(self.get_mut().next_chunk())
    }
}
