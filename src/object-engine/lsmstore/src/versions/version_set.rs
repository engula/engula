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

use std::{collections::VecDeque, io, path::PathBuf, sync::Arc};

use prost::Message;
use tokio::{fs, io::AsyncWriteExt, sync::Mutex};

use crate::{
    versions::{proto::*, version::Version, *},
    *,
};

pub struct VersionSet {
    inner: Arc<Mutex<Inner>>,
}

struct Inner {
    tenant: String,
    base_dir: PathBuf,
    versions: VecDeque<Version>,
    manifest: Option<manifest::Writer>,
    next_file_num: u64,
    current_file_num: u64,
}

impl VersionSet {
    pub async fn new(base_dir: impl Into<PathBuf>, tenant: &str) -> Result<Self> {
        let mut inner = Inner {
            tenant: tenant.to_owned(),
            base_dir: base_dir.into(),
            versions: VecDeque::new(),
            manifest: None,
            next_file_num: 0,
            current_file_num: 0,
        };
        match inner.find_current_manifest().await? {
            Some(last_file_num) => {
                inner.load(last_file_num).await?;
            }
            None => {
                inner.create().await?;
            }
        }
        Ok(Self {
            inner: Arc::new(Mutex::new(inner)),
        })
    }
}

impl VersionSet {
    pub async fn current_version(&self) -> Version {
        let inner = self.inner.lock().await;
        inner.current_version()
    }

    #[allow(dead_code)]
    pub async fn log_and_apply(&self, ve: VersionEdit) -> Result<()> {
        let mut inner = self.inner.lock().await;

        let rolleded = if match &inner.manifest {
            Some(manifest) => manifest.accumulated_size().await > manifest::MAX_FILE_SIZE,
            None => true, // new or restarted
        } {
            let new_file_num = inner.get_next_file_num();
            let next_file_num = inner.next_file_num;
            inner.create_manifest(new_file_num, next_file_num).await?;
            inner.current_file_num = new_file_num;
            true
        } else {
            false
        };

        let mut manifest = inner.manifest.take().unwrap();
        manifest.append(&ve.encode_to_vec()).await?;
        manifest.flush_and_sync().await?;
        inner.manifest = Some(manifest);
        if rolleded {
            inner.update_current(inner.current_file_num).await?;
        }

        let mut new_version = inner.current_version().to_owned();
        new_version.apply(&ve).await?;

        inner.versions.push_back(new_version);

        Ok(())
    }
}

impl Inner {
    fn get_next_file_num(&mut self) -> u64 {
        let num = self.next_file_num;
        self.next_file_num += 1;
        num
    }

    fn current_version(&self) -> Version {
        self.versions.back().unwrap().to_owned()
    }

    async fn create_manifest(&mut self, create_file_num: u64, next_file_num: u64) -> Result<()> {
        let file = {
            let filename = self.file_path(create_file_num);
            fs::create_dir_all(filename.parent().unwrap()).await?;
            fs::OpenOptions::new()
                .create(true)
                .append(true)
                .write(true)
                .open(filename)
                .await?
        };
        let mut writer = manifest::Writer::new(file);

        let snapshot = self
            .current_version()
            .generate_snapshot(next_file_num)
            .await;

        writer.append(&snapshot.encode_to_vec()).await?;

        self.manifest = Some(writer);
        Ok(())
    }

    fn file_path(&self, manifest_file_name: u64) -> PathBuf {
        self.base_dir
            .join(&self.tenant)
            .join(format!("MANIFEST-{}", manifest_file_name))
    }

    async fn create(&mut self) -> Result<()> {
        let new_version = Version::default();
        self.versions.push_back(new_version);
        self.current_file_num = self.get_next_file_num();
        let next_file_num = self.next_file_num;
        self.create_manifest(self.current_file_num, next_file_num)
            .await?;
        let mut manifest = self.manifest.take().unwrap();
        manifest.flush_and_sync().await?;
        self.manifest = Some(manifest);
        self.update_current(self.current_file_num).await?;
        Ok(())
    }

    async fn load(&mut self, manifest_file_num: u64) -> Result<()> {
        self.current_file_num = manifest_file_num;
        let manifest_file = {
            let filename = self.file_path(self.current_file_num);
            fs::create_dir_all(filename.parent().unwrap()).await?;
            fs::OpenOptions::new()
                .create(false)
                .append(true)
                .read(true)
                .open(filename)
                .await?
        };

        let mut new_version = Version::default();
        let mut r = manifest::Reader::new(manifest_file);
        loop {
            let res = r.read().await;
            if let Err(Error::Io(err)) = &res {
                if Self::is_eof(err) {
                    break;
                }
            }
            let chunk_data = res?;
            let ve = VersionEdit::decode(chunk_data.as_slice()).unwrap();
            if ve.next_file_num > 0 {
                self.next_file_num = ve.next_file_num;
            }
            new_version.apply(&ve).await?;
        }
        self.versions.push_back(new_version);
        Ok(())
    }

    fn is_eof(err: &io::Error) -> bool {
        err.kind() == io::ErrorKind::UnexpectedEof
    }

    async fn find_current_manifest(&self) -> Result<Option<u64>> {
        let path = self.base_dir.join(&self.tenant).join("CURRENT");
        let res = fs::read_to_string(&path).await;
        if let Err(io_err) = &res {
            if io_err.kind() == std::io::ErrorKind::NotFound {
                return Ok(None);
            }
        }
        let content = res?;
        if !content.ends_with('\n') {
            return Err(Error::Corrupted("invalid CURRENT".to_string()));
        }
        let manifest_name = content.trim().split_once('-');
        if manifest_name.is_none() {
            return Err(Error::Corrupted("invalid CURRENT".to_string()));
        }
        let (prefix, num) = manifest_name.unwrap();
        if prefix != "MANIFEST" {
            return Err(Error::Corrupted("invalid CURRENT".to_string()));
        }
        let num = num.parse::<u64>();
        if num.is_err() {
            return Err(Error::Corrupted("invalid CURRENT".to_string()));
        }
        Ok(Some(num.unwrap()))
    }

    async fn update_current(&self, file_num: u64) -> Result<()> {
        let tmp_path = self
            .base_dir
            .join(&self.tenant)
            .join(format!("CURRENT.{}.dbtmp", file_num));
        let curr_path = self.base_dir.join(&self.tenant).join("CURRENT");
        let _ = fs::remove_file(&tmp_path).await;
        {
            let mut tmp_w = fs::OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&tmp_path)
                .await?;
            tmp_w
                .write_all(format!("MANIFEST-{}\n", file_num).as_bytes())
                .await?;
            tmp_w.sync_all().await?;
        }
        fs::rename(tmp_path, curr_path).await?;
        Ok(())
    }
}
