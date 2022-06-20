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
    path::Path,
    pin::Pin,
    task::{Context, Poll},
};

use futures::Stream;
use tonic::Status;

use super::{SnapManager, SnapshotGuard};
use crate::{
    serverpb::v1::{snapshot_chunk, SnapshotChunk, SnapshotMeta},
    Error, Result,
};

type SnapResult = std::result::Result<SnapshotChunk, Status>;

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
                        break;
                    }
                }
                let value = snapshot_chunk::Value::ChunkData(chunk_data);
                Some(Ok(SnapshotChunk { value: Some(value) }))
            }
            None if self.file_index >= self.info.meta.files.len() => None,
            None => {
                use std::os::unix::ffi::OsStrExt;
                let file_meta = &self.info.meta.files[self.file_index];
                let path = Path::new(OsStr::from_bytes(&file_meta.name));
                match OpenOptions::new().open(&path) {
                    Ok(file) => self.file = Some(file),
                    Err(err) => return Some(Err(err.into())),
                }
                let value = snapshot_chunk::Value::File(file_meta.to_owned());
                Some(Ok(SnapshotChunk { value: Some(value) }))
            }
        }
    }
}

impl futures::Stream for SnapshotChunkStream {
    type Item = std::result::Result<SnapshotChunk, Status>;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(self.get_mut().next_chunk())
    }
}
