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

use std::path::Path;

use futures::{channel::mpsc, SinkExt};
use prost::Message;
use tracing::{error, info};

use super::{SnapManager, SNAP_DATA};
use crate::{
    raftgroup::{
        fsm::SnapshotBuilder,
        snap::{SNAP_META, SNAP_TEMP},
        worker::Request,
        StateMachine,
    },
    runtime::{Executor, TaskPriority},
    serverpb::v1::{SnapshotFile, SnapshotMeta},
    Result,
};

pub fn dispatch_creating_snap_task(
    executor: &Executor,
    replica_id: u64,
    mut sender: mpsc::Sender<Request>,
    state_machine: &impl StateMachine,
    snap_mgr: SnapManager,
) {
    let builder = state_machine.snapshot_builder();
    executor.spawn(None, TaskPriority::IoLow, async move {
        match create_snapshot(replica_id, &snap_mgr, builder).await {
            Ok(_) => {
                info!("replica {replica_id} create snapshot success");
            }
            Err(err) => {
                error!("replica {replica_id} create snapshot: {err}");
            }
        };

        sender
            .send(Request::CreateSnapshotFinished)
            .await
            .unwrap_or_default();
    });
}

/// Create new snapshot and returns snapshot id.
pub(super) async fn create_snapshot(
    replica_id: u64,
    snap_mgr: &SnapManager,
    builder: Box<dyn SnapshotBuilder>,
) -> Result<Vec<u8>> {
    let snap_dir = snap_mgr.create(replica_id);
    info!(
        "replica {replica_id} begin create snapshot at {}",
        snap_dir.display()
    );

    let data = snap_dir.join(SNAP_DATA);
    let (apply_state, descriptor) = builder.checkpoint(&data).await?;
    if !std::fs::try_exists(&data)? {
        panic!("Checkpoint did not generate any data.");
    }

    let mut files = vec![];
    if data.is_dir() {
        for entry in std::fs::read_dir(data)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                panic!("Not supported");
            }
            files.push(read_file_meta(&path).await?);
        }
    } else {
        files.push(read_file_meta(&data).await?);
    }

    let snap_meta = SnapshotMeta {
        apply_state: Some(apply_state),
        group_desc: Some(descriptor),
        files,
    };

    stable_snapshot_meta(&snap_dir, &snap_meta).await?;

    info!(
        "replica {replica_id} create snapshot {} success",
        snap_dir.display()
    );

    Ok(snap_mgr.install(replica_id, &snap_dir, &snap_meta))
}

pub(super) async fn stable_snapshot_meta(base_dir: &Path, snap_meta: &SnapshotMeta) -> Result<()> {
    use std::{fs::OpenOptions, io::Write};

    let content = snap_meta.encode_to_vec();

    let tmp = base_dir.join(SNAP_TEMP);
    let mut file = OpenOptions::new().write(true).create(true).open(&tmp)?;
    file.write_all(&content)?;
    file.sync_all()?;
    drop(file);

    let meta = base_dir.join(SNAP_META);
    std::fs::rename(tmp, meta)?;

    std::fs::File::open(base_dir)?.sync_all()?;

    Ok(())
}

async fn read_file_meta(filename: &Path) -> Result<SnapshotFile> {
    use std::{
        fs::OpenOptions,
        io::{ErrorKind, Read},
    };

    let mut buf = vec![0; 4096];
    let mut file = OpenOptions::new().read(true).open(filename)?;
    let mut hasher = crc32fast::Hasher::new();

    let mut size: u64 = 0;
    let mut count = 0;
    loop {
        let n = match file.read(&mut buf) {
            Ok(n) => n,
            Err(e) if e.kind() == ErrorKind::Interrupted => continue,
            Err(e) => return Err(e.into()),
        };
        if n == 0 {
            break;
        }

        size += n as u64;
        count += 1;
        hasher.update(&buf[..n]);
        if count % 10 == 0 {
            crate::runtime::yield_now().await;
        }
    }

    let name = if filename.file_name().unwrap() == SNAP_DATA {
        Path::new(SNAP_DATA).to_path_buf()
    } else {
        Path::new(SNAP_DATA).join(filename.file_name().unwrap())
    };
    let crc32 = hasher.finalize();

    Ok(SnapshotFile {
        name: name.to_str().unwrap().as_bytes().to_owned(),
        crc32,
        size,
    })
}
