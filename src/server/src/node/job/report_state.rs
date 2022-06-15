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

use std::time::Duration;

use engula_api::server::v1::{
    report_request::GroupUpdates, root_client::RootClient, GroupDesc, ReplicaState, ReportRequest,
};
use futures::{channel::mpsc, StreamExt};
use tracing::warn;

use crate::{
    node::StateEngine,
    runtime::{Executor, TaskPriority},
    Error, Result,
};

#[derive(Clone)]
pub struct StateChannel {
    sender: mpsc::UnboundedSender<GroupUpdates>,
}

pub fn setup(executor: &Executor, state_engine: StateEngine) -> StateChannel {
    let (sender, receiver) = mpsc::unbounded();

    executor.spawn(None, TaskPriority::IoHigh, async move {
        report_state_worker(receiver, state_engine).await;
    });

    StateChannel { sender }
}

async fn report_state_worker(
    mut receiver: mpsc::UnboundedReceiver<GroupUpdates>,
    state_engine: StateEngine,
) {
    let mut roots = load_roots(&state_engine).await;
    while let Some(updates) = wait_state_updates(&mut receiver).await {
        let req = ReportRequest { updates };
        report_state_updates(&mut roots, req).await;
    }
}

/// Wait until at least a new request is received or the channel is closed. Returns `None` if the
/// channel is closed.
async fn wait_state_updates(
    receiver: &mut mpsc::UnboundedReceiver<GroupUpdates>,
) -> Option<Vec<GroupUpdates>> {
    use prost::Message;

    // TODO(walter) skip root group?
    if let Some(update) = receiver.next().await {
        let mut size = update.encoded_len();
        let mut updates = vec![update];
        while size < 32 * 1024 {
            match receiver.try_next() {
                Ok(Some(update)) => {
                    size += update.encoded_len();
                    updates.push(update);
                }
                _ => break,
            }
        }
        return Some(updates);
    }
    None
}

async fn load_roots(state_engine: &StateEngine) -> Vec<String> {
    loop {
        if let Some(roots) = state_engine.load_root_nodes().await.unwrap() {
            return roots.into_iter().map(|d| d.addr).collect();
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn report_state_updates(roots: &mut Vec<String>, request: ReportRequest) {
    'OUTER: loop {
        for root in roots.iter() {
            match issue_report_request(root.to_owned(), &request).await {
                Ok(()) => return,
                Err(Error::NotRootLeader(recommends)) => {
                    *roots = recommends;
                    continue 'OUTER;
                }
                Err(err) => {
                    warn!("report state updates: {}, root {}", err, root);
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Issue report rpc request to root server.
///
/// This function is executed synchronously, and it will not affect normal reporting, because
/// a node has only a small number of replicas, and the replica state changes are not frequent.
/// Using a synchronous method can simplify the sequence problem introduced by asynchronous
/// reporting.
///
/// If one day you find that reporting has become a bottleneck, you can consider optimizing this
/// code.
async fn issue_report_request(addr: String, request: &ReportRequest) -> Result<()> {
    let mut client = RootClient::connect(addr).await?;
    client.report(request.clone()).await?;
    Ok(())
}

impl StateChannel {
    #[inline]
    pub fn broadcast_replica_state(&mut self, group_id: u64, replica_state: ReplicaState) {
        let update = GroupUpdates {
            group_id,
            group_desc: None,
            replica_state: Some(replica_state),
        };
        self.sender.start_send(update).unwrap_or_default();
    }

    #[inline]
    pub fn broadcast_group_descriptor(&mut self, group_id: u64, group_desc: GroupDesc) {
        let update = GroupUpdates {
            group_id,
            group_desc: Some(group_desc),
            replica_state: None,
        };
        self.sender.start_send(update).unwrap_or_default();
    }
}
