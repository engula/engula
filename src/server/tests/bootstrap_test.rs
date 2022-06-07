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

use std::{sync::mpsc, thread};

use engula_server::{runtime::ExecutorOwner, Result};
use tempdir::TempDir;

#[test]
fn bootstrap_cluster() -> Result<()> {
    tracing_subscriber::fmt::init();

    let (sender, first_node_receiver) = mpsc::channel();
    thread::spawn(|| {
        let owner = ExecutorOwner::new(1);
        let tmp_dir = TempDir::new("bootstrap-node-1").unwrap().into_path();

        engula_server::run(
            owner.executor(),
            tmp_dir,
            "localhost:0".to_string(),
            true,
            vec![],
            Some(sender),
        )
        .unwrap()
    });

    first_node_receiver.recv().unwrap();

    // At this point, initialization has been completed.
    Ok(())
}
