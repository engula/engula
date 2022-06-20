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
    fs::File,
    path::{Path, PathBuf},
};

use engula_api::server::v1::ReplicaDesc;
use futures::{channel::mpsc, SinkExt, StreamExt};
use raft::eraftpb::Message;
use tracing::error;

use super::SnapManager;
use crate::{
    node::{
        replica::raft::{retrive_snapshot, worker::Request},
        TransportManager,
    },
    runtime::{Executor, TaskPriority},
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
    base_dir: PathBuf,
    meta: SnapshotMeta,
    file_name: Vec<u8>,
    file: Option<PartialFile>,
}

impl SnapshotBuilder {
    fn new(base_dir: &Path) -> Self {
        SnapshotBuilder {
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
                Some(file) => file.write_all(&data).await,
                None => Err(Error::InvalidData("missing file meta".to_string())),
            },
            None => Ok(()),
        }
    }

    async fn switch_file(&mut self, file_meta: SnapshotFile) -> Result<()> {
        use std::{ffi::OsString, os::unix::ffi::OsStringExt};

        self.finish_partial_file().await?;

        let name = file_meta.name.clone();
        let str = OsString::from_vec(name.clone());
        let path = self.base_dir.join(str);

        self.file_name = name;
        self.file = Some(PartialFile::new(&path, file_meta)?);

        Ok(())
    }

    async fn finish_partial_file(&mut self) -> Result<()> {
        if let Some(file) = self.file.take() {
            self.meta.files.push(file.finish().await?);
        }
        Ok(())
    }

    async fn finish(mut self) -> Result<SnapshotMeta> {
        use std::fs::OpenOptions;

        self.finish_partial_file().await?;
        OpenOptions::new().open(&self.base_dir)?.sync_all()?;
        Ok(self.meta)
    }
}

impl PartialFile {
    fn new(path: &Path, file_meta: SnapshotFile) -> Result<Self> {
        use std::fs::OpenOptions;

        let file = OpenOptions::new().write(true).open(path)?;

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
        let crc32 = self.crc32.finalize();
        self.file.sync_all()?;
        Ok(SnapshotFile {
            name: vec![], // FIXME(walter) name prefix
            crc32,
            size: self.size as u64,
        })
    }
}

pub fn dispatch_downloading_snap_task(
    executor: &Executor,
    replica_id: u64,
    mut sender: mpsc::Sender<Request>,
    snap_mgr: SnapManager,
    tran_mgr: TransportManager,
    from_replica: ReplicaDesc,
    mut msg: Message,
) {
    executor.spawn(None, TaskPriority::IoLow, async move {
        match download_snap(replica_id, tran_mgr, snap_mgr, from_replica, &msg).await {
            Ok(snap_id) => {
                msg.snapshot.as_mut().unwrap().data = snap_id;
                let request = Request::InstallSnapshot { msg };
                sender.send(request).await.unwrap_or_default();
            }
            Err(err) => {
                error!("replica {} download snapshot: {}", replica_id, err);
                let request = Request::RejectSnapshot { msg };
                sender.send(request).await.unwrap_or_default();
            }
        };
    });
}

/// Download snapshot from target and returns the local snapshot id.
async fn download_snap(
    replica_id: u64,
    tran_mgr: TransportManager,
    snap_mgr: SnapManager,
    from_replica: ReplicaDesc,
    msg: &Message,
) -> Result<Vec<u8>> {
    assert!(msg.has_snapshot() && !msg.get_snapshot().is_empty());
    let snapshot = msg.get_snapshot();
    let snapshot_id = snapshot.data.clone();
    let chunk_stream = retrive_snapshot(&tran_mgr, from_replica, snapshot_id).await?;
    save_snapshot(&snap_mgr, replica_id, chunk_stream).await
}

async fn save_snapshot<S>(
    snap_mgr: &SnapManager,
    replica_id: u64,
    mut chunk_stream: S,
) -> Result<Vec<u8>>
where
    S: futures::Stream<Item = std::result::Result<SnapshotChunk, tonic::Status>> + Unpin,
{
    let base_dir = snap_mgr.create(replica_id);
    let mut snap_builder = SnapshotBuilder::new(&base_dir);
    while let Some(resp) = chunk_stream.next().await {
        let chunk = resp?;
        snap_builder.append(chunk).await?;
    }

    let snap_meta = snap_builder.finish().await?;
    snap_mgr.install(replica_id, &base_dir, &snap_meta);

    use std::os::unix::ffi::OsStringExt;

    Ok(base_dir.as_os_str().to_owned().into_vec())
}

#[cfg(test)]
mod tests {
    #[test]
    fn download_snapshot() {
        // TODO
    }
}
