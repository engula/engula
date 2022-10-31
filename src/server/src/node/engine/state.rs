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

use crate::{constants::STATE_REPLICA_ID, serverpb::v1::*, Result};

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
    raw: Arc<raft_engine::Engine>,
}

impl StateEngine {
    pub fn new(raw: Arc<raft_engine::Engine>) -> Self {
        StateEngine { raw }
    }

    /// Read node ident from engine. `None` is returned if no such ident exists.
    pub async fn read_ident(&self) -> Result<Option<NodeIdent>> {
        Ok(self
            .raw
            .get_message::<NodeIdent>(STATE_REPLICA_ID, keys::node_ident())?)
    }

    /// Save node ident, return appropriate error if ident already exists.
    pub async fn save_ident(&self, ident: &NodeIdent) -> Result<()> {
        use raft_engine::LogBatch;

        let mut lb = LogBatch::default();
        lb.put_message(STATE_REPLICA_ID, keys::node_ident().to_owned(), ident)
            .expect("NodeIdent is Serializable");
        self.raw.write(&mut lb, false)?;
        Ok(())
    }

    /// Save root desc.
    pub async fn save_root_desc(&self, root_desc: RootDesc) -> Result<()> {
        use raft_engine::LogBatch;

        let mut lb = LogBatch::default();
        lb.put_message(STATE_REPLICA_ID, keys::root_desc().to_owned(), &root_desc)
            .expect("RootDesc is Serializable");
        self.raw.write(&mut lb, false)?;
        Ok(())
    }

    /// Load root desc. `None` is returned if there no any root node records exists.
    pub async fn load_root_desc(&self) -> Result<Option<RootDesc>> {
        Ok(self
            .raw
            .get_message::<RootDesc>(STATE_REPLICA_ID, keys::root_desc())?)
    }

    /// Save replica state.
    pub async fn save_replica_state(
        &self,
        group_id: u64,
        replica_id: u64,
        state: ReplicaLocalState,
    ) -> Result<()> {
        use raft_engine::LogBatch;

        let replica_meta = ReplicaMeta {
            group_id,
            replica_id,
            state: state.into(),
        };

        let mut lb = LogBatch::default();
        let state_key = keys::replica_state(replica_id);
        lb.put_message(STATE_REPLICA_ID, state_key.to_vec(), &replica_meta)
            .expect("RootDesc is Serializable");
        self.raw.write(&mut lb, false)?;
        Ok(())
    }

    /// Fetch all replica states.
    pub async fn replica_states(&self) -> Result<Vec<(u64, u64, ReplicaLocalState)>> {
        let mut replica_states = Vec::default();
        let start_key = keys::replica_state_prefix();
        let end_key = keys::replica_state_end();
        self.raw.scan_messages(
            STATE_REPLICA_ID,
            Some(start_key),
            Some(end_key),
            false,
            |_, replica_meta: ReplicaMeta| {
                let replica_id = replica_meta.replica_id;
                let group_id = replica_meta.group_id;
                let local_state = ReplicaLocalState::from_i32(replica_meta.state)
                    .expect("invalid ReplicaLocalState value");
                replica_states.push((group_id, replica_id, local_state));
                true
            },
        )?;

        Ok(replica_states)
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
}
