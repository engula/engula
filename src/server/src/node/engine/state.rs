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

use std::sync::Arc;

use engula_api::server::v1::*;
use prost::Message;

use super::RawDb;
use crate::{serverpb::v1::*, Result};

const STATE_CF_NAME: &str = "state";

/// A structure supports saving and loading local states.
///
/// Local states:
/// - node ident
/// - root node descriptors
/// - replica states
///
/// NOTE: The group descriptors is stored in the corresponding GroupEngine, which is to ensure
/// that both the changes of group descriptor and data are persisted to disk in atomic.
#[derive(Clone)]
pub struct StateEngine
where
    Self: Send + Sync,
{
    raw_db: Arc<RawDb>,
}

pub struct ReplicaStateIterator<'a> {
    inner: rocksdb::DBIteratorWithThreadMode<'a, rocksdb::DB>,
}

impl StateEngine {
    pub fn new(raw_db: Arc<RawDb>) -> Result<Self> {
        if raw_db.cf_handle(STATE_CF_NAME).is_none() {
            raw_db.create_cf(STATE_CF_NAME)?;
        }
        Ok(StateEngine { raw_db })
    }

    /// Read node ident from engine. `None` is returned if no such ident exists.
    pub async fn read_ident(&self) -> Result<Option<NodeIdent>> {
        let cf_handle = self
            .raw_db
            .cf_handle(STATE_CF_NAME)
            .expect("state column family");
        match self.raw_db.get_pinned_cf(&cf_handle, keys::node_ident())? {
            Some(value) => {
                let ident = NodeIdent::decode(value.as_ref()).expect("valid node ident format");
                Ok(Some(ident))
            }
            None => Ok(None),
        }
    }

    /// Save node ident, return appropriate error if ident already exists.
    pub async fn save_ident(&self, ident: &NodeIdent) -> Result<()> {
        use rocksdb::{WriteBatch, WriteOptions};

        // FIXME(walter) check existence.
        let cf_handle = self
            .raw_db
            .cf_handle(STATE_CF_NAME)
            .expect("state column family");

        let mut opts = WriteOptions::default();
        opts.set_sync(true);
        let mut wb = WriteBatch::default();
        wb.put_cf(&cf_handle, keys::node_ident(), ident.encode_to_vec());
        self.raw_db.write_opt(wb, &opts)?;

        Ok(())
    }

    /// Save root desc.
    pub async fn save_root_desc(&self, root_desc: RootDesc) -> Result<()> {
        use rocksdb::{WriteBatch, WriteOptions};

        let cf_handle = self
            .raw_db
            .cf_handle(STATE_CF_NAME)
            .expect("state column family");

        let mut opts = WriteOptions::default();
        opts.set_sync(true);
        let mut wb = WriteBatch::default();
        wb.put_cf(&cf_handle, keys::root_desc(), root_desc.encode_to_vec());
        self.raw_db.write_opt(wb, &opts)?;

        Ok(())
    }

    /// Load root desc. `None` is returned if there no any root node records exists.
    pub async fn load_root_desc(&self) -> Result<Option<RootDesc>> {
        let cf_handle = self
            .raw_db
            .cf_handle(STATE_CF_NAME)
            .expect("state column family");
        match self.raw_db.get_pinned_cf(&cf_handle, keys::root_desc())? {
            Some(value) => Ok(Some(RootDesc::decode(value.as_ref())?)),
            None => Ok(None),
        }
    }

    /// Save replica state.
    pub async fn save_replica_state(
        &self,
        group_id: u64,
        replica_id: u64,
        state: ReplicaLocalState,
    ) -> Result<()> {
        use rocksdb::{WriteBatch, WriteOptions};

        let replica_meta = ReplicaMeta {
            group_id,
            replica_id,
            state: state.into(),
        };
        let cf_handle = self
            .raw_db
            .cf_handle(STATE_CF_NAME)
            .expect("state column family");

        let mut opts = WriteOptions::default();
        opts.set_sync(true);
        let mut wb = WriteBatch::default();
        let state_key = keys::replica_state(replica_id);
        wb.put_cf(
            &cf_handle,
            state_key.as_slice(),
            replica_meta.encode_to_vec(),
        );
        self.raw_db.write_opt(wb, &opts)?;

        Ok(())
    }

    /// Iterate group states.
    pub async fn iterate_replica_states(&self) -> ReplicaStateIterator<'_> {
        use rocksdb::{Direction, IteratorMode};

        let cf_handle = self
            .raw_db
            .cf_handle(STATE_CF_NAME)
            .expect("state column family");
        let mode = IteratorMode::From(keys::replica_state_prefix(), Direction::Forward);
        let it = self.raw_db.iterator_cf(&cf_handle, mode);
        ReplicaStateIterator { inner: it }
    }
}

impl<'a> Iterator for ReplicaStateIterator<'a> {
    /// (group id, replica id, replica state)
    type Item = Result<(u64, u64, ReplicaLocalState)>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.inner.next()? {
            Ok((key, value)) => {
                if !key.starts_with(keys::replica_state_end()) {
                    let replica_id = keys::parse_replica_id(&key).expect("valid replica state key");
                    let replica_meta =
                        ReplicaMeta::decode(value.as_ref()).expect("valid ReplicaMeta format");
                    Some(Ok((
                        replica_meta.group_id,
                        replica_id,
                        ReplicaLocalState::from_i32(replica_meta.state)
                            .expect("valid ReplicaLocalState value"),
                    )))
                } else {
                    None
                }
            }
            Err(err) => Some(Err(err.into())),
        }
    }
}

mod keys {
    const IDENT_KEY: &[u8] = &[0x1];
    const ROOT_DESCRIPTOR_KEY: &[u8] = &[0x2];
    const REPLICA_STATE_PREFIX: &[u8] = &[0x3];
    const REPLICA_STATE_END: &[u8] = &[0x4];

    pub fn node_ident() -> &'static [u8] {
        IDENT_KEY
    }

    pub fn root_desc() -> &'static [u8] {
        ROOT_DESCRIPTOR_KEY
    }

    pub fn replica_state_prefix() -> &'static [u8] {
        REPLICA_STATE_PREFIX
    }

    pub fn replica_state_end() -> &'static [u8] {
        REPLICA_STATE_END
    }

    pub fn replica_state(replica_id: u64) -> [u8; 9] {
        let mut buf = [0; 9];
        buf[..1].copy_from_slice(REPLICA_STATE_PREFIX);
        buf[1..].copy_from_slice(&replica_id.to_le_bytes());
        buf
    }

    /// Parse replica id from replica state key. `None` is returned if the prefix or length does not
    /// matched.
    pub fn parse_replica_id(key: &[u8]) -> Option<u64> {
        if key.len() == 9 && key.starts_with(REPLICA_STATE_PREFIX) {
            let mut buf = [0; 8];
            buf.copy_from_slice(&key[1..]);
            Some(u64::from_le_bytes(buf))
        } else {
            None
        }
    }
}
