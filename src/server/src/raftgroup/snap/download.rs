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
    ffi::OsString,
    fs::File,
    os::unix::ffi::OsStringExt,
    path::{Path, PathBuf},
};

use engula_api::server::v1::ReplicaDesc;
use futures::{channel::mpsc, SinkExt, StreamExt};
use raft::eraftpb::Message;
use tracing::{debug, error, info};

use super::SnapManager;
use crate::{
    raftgroup::{metrics::*, retrive_snapshot, worker::Request, ChannelManager},
    record_latency,
    runtime::TaskPriority,
    serverpb::v1::{snapshot_chunk, SnapshotChunk, SnapshotFile, SnapshotMeta},
    Error, Result,
};

struct PartialFile {
    meta: SnapshotFile,
    file: File,
    size: usize,
    crc32: crc32fast::Hasher,
}

struct SnapshotBuilder {
    replica_id: u64,
    base_dir: PathBuf,
    meta: SnapshotMeta,
    file_name: Vec<u8>,
    file: Option<PartialFile>,
}

impl SnapshotBuilder {
    fn new(replica_id: u64, base_dir: &Path) -> Self {
        SnapshotBuilder {
            replica_id,
            base_dir: base_dir.to_owned(),
            meta: SnapshotMeta::default(),
            file_name: vec![],
            file: None,
        }
    }

    async fn append(&mut self, chunk: SnapshotChunk) -> Result<()> {
        match chunk.value {
            Some(snapshot_chunk::Value::File(file)) => self.switch_file(file).await,
            Some(snapshot_chunk::Value::ChunkData(data)) => match self.file.as_mut() {
                Some(file) => {
                    RAFTGROUP_DOWNLOAD_SNAPSHOT_BYTES_TOTAL.inc_by(data.len() as u64);
                    file.write_all(&data).await
                }
                None => Err(Error::InvalidData("missing file meta".to_string())),
            },
            Some(snapshot_chunk::Value::Meta(meta)) => {
                self.meta.apply_state = meta.apply_state;
                self.meta.group_desc = meta.group_desc;
                Ok(())
            }
            None => Ok(()),
        }
    }

    async fn switch_file(&mut self, file_meta: SnapshotFile) -> Result<()> {
        self.finish_partial_file().await?;

        let name = file_meta.name.clone();
        let str = OsString::from_vec(name.clone());
        let path = self.base_dir.join(str);
        if let Some(parent) = path.parent() {
            if !std::fs::try_exists(parent)? {
                std::fs::create_dir_all(parent)?;
            }
        }

        self.file_name = name;
        self.file = Some(PartialFile::new(self.replica_id, &path, file_meta)?);

        Ok(())
    }

    async fn finish_partial_file(&mut self) -> Result<()> {
        if let Some(file) = self.file.take() {
            self.meta.files.push(file.finish().await?);
        }
        Ok(())
    }

    async fn finish(mut self) -> Result<SnapshotMeta> {
        self.finish_partial_file().await?;
        super::create::stable_snapshot_meta(&self.base_dir, &self.meta).await?;
        Ok(self.meta)
    }
}

impl PartialFile {
    fn new(replica_id: u64, path: &Path, file_meta: SnapshotFile) -> Result<Self> {
        use std::fs::OpenOptions;

        debug!(
            "replica {replica_id} receive snapshot file {}, size {}, crc32 {}",
            path.display(),
            file_meta.size,
            file_meta.crc32
        );

        let file = OpenOptions::new().write(true).create(true).open(path)?;

        Ok(PartialFile {
            meta: file_meta,
            file,
            size: 0,
            crc32: crc32fast::Hasher::new(),
        })
    }

    async fn write_all(&mut self, buf: &[u8]) -> Result<()> {
        use std::io::Write;

        self.file.write_all(buf)?;
        self.crc32.update(buf);
        self.size += buf.len();
        Ok(())
    }

    async fn finish(self) -> Result<SnapshotFile> {
        self.file.sync_all()?;

        if self.size as u64 != self.meta.size {
            return Err(Error::InvalidData(format!(
                "invalid size of file, expect {}, but got {}",
                self.meta.size, self.size
            )));
        }

        let crc32 = self.crc32.finalize();
        if crc32 != self.meta.crc32 {
            return Err(Error::InvalidData(format!(
                "checksum is not equals, expect {}, but got {}",
                self.meta.size, self.size
            )));
        }

        Ok(self.meta)
    }
}

pub fn dispatch_downloading_snap_task(
    replica_id: u64,
    mut sender: mpsc::Sender<Request>,
    snap_mgr: SnapManager,
    tran_mgr: ChannelManager,
    from_replica: ReplicaDesc,
    mut msg: Message,
) {
    crate::runtime::current().spawn(None, TaskPriority::IoLow, async move {
        match download_snap(replica_id, tran_mgr, snap_mgr, from_replica, &msg).await {
            Ok(snap_id) => {
                msg.snapshot.as_mut().unwrap().data = snap_id;
                let request = Request::InstallSnapshot { msg };
                sender.send(request).await.unwrap_or_default();
            }
            Err(err) => {
                error!("replica {replica_id} download snapshot: {err}");
                let request = Request::RejectSnapshot { msg };
                sender.send(request).await.unwrap_or_default();
            }
        };
    });
}

/// Download snapshot from target and returns the local snapshot id.
async fn download_snap(
    replica_id: u64,
    tran_mgr: ChannelManager,
    snap_mgr: SnapManager,
    from_replica: ReplicaDesc,
    msg: &Message,
) -> Result<Vec<u8>> {
    record_latency!(take_download_snapshot_metrics());
    assert!(msg.has_snapshot() && !msg.get_snapshot().is_empty());
    let snapshot = msg.get_snapshot();
    let snapshot_id = snapshot.data.clone();
    let chunk_stream = retrive_snapshot(&tran_mgr, from_replica, snapshot_id).await?;
    save_snapshot(&snap_mgr, replica_id, chunk_stream).await
}

pub(super) async fn save_snapshot<S>(
    snap_mgr: &SnapManager,
    replica_id: u64,
    mut chunk_stream: S,
) -> Result<Vec<u8>>
where
    S: futures::Stream<Item = Result<SnapshotChunk, tonic::Status>> + Unpin,
{
    let base_dir = snap_mgr.create(replica_id);
    info!(
        "replica {replica_id} save incoming snapshot chunk stream into {}",
        base_dir.display()
    );

    std::fs::create_dir_all(&base_dir)?;
    let mut snap_builder = SnapshotBuilder::new(replica_id, &base_dir);
    while let Some(resp) = chunk_stream.next().await {
        let chunk = resp?;
        snap_builder.append(chunk).await?;
    }

    let snap_meta = snap_builder.finish().await?;
    Ok(snap_mgr.install(replica_id, &base_dir, &snap_meta))
}
