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

use std::{net::SocketAddr, sync::mpsc, thread, time::Duration};

use engula_server::{runtime::ExecutorOwner, Result};
use tempdir::TempDir;

#[ctor::ctor]
fn init() {
    tracing_subscriber::fmt::init();
}

fn spawn_server(
    name: &'static str,
    init: bool,
    join_list: Vec<String>,
) -> mpsc::Receiver<SocketAddr> {
    let (sender, receiver) = mpsc::channel();
    thread::spawn(move || {
        let owner = ExecutorOwner::new(1);
        let tmp_dir = TempDir::new(name).unwrap().into_path();

        engula_server::run(
            owner.executor(),
            tmp_dir,
            "localhost:0".to_string(),
            init,
            join_list,
            Some(sender),
        )
        .unwrap()
    });
    receiver
}

#[test]
fn bootstrap_cluster() -> Result<()> {
    let receiver = spawn_server("bootstrap-node", true, vec![]);
    receiver.recv().unwrap();

    // FIXME(walter) find a more efficient way to detect leader elections.
    thread::sleep(Duration::from_secs(2));

    // At this point, initialization has been completed.
    Ok(())
}

#[test]
fn join_node() -> Result<()> {
    let node_1_addr = spawn_server("join-node-1", true, vec![]).recv().unwrap();
    let _node_2_addr = spawn_server("join-node-2", false, vec![node_1_addr.to_string()])
        .recv()
        .unwrap();

    // FIXME(walter) find a more efficient way to detect leader elections.
    thread::sleep(Duration::from_secs(2));

    Ok(())
}
