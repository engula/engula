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

use engula_api::server::v1::{node_client::NodeClient, *};

use super::{Root, Schema};
use crate::Result;

impl Root {
    pub async fn send_heartbeat(&self, schema: Schema) -> Result<()> {
        let cur_node_id = self.current_node_id();
        let nodes = schema.list_node().await?;

        let mut piggybacks = Vec::new();

        // TODO: no need piggyback root info everytime.
        if true {
            let mut roots = schema.get_root_replicas().await?;
            roots.move_first(cur_node_id);
            piggybacks.push(PiggybackRequest {
                info: Some(piggyback_request::Info::SyncRoot(SyncRootRequest {
                    roots: roots.into(),
                })),
            });
        }

        // TODO: collect stats and group detail.

        for n in nodes {
            let mut client = NodeClient::connect(n.addr).await?;
            let res = client
                .root_heartbeat(HeartbeatRequest {
                    piggybacks: piggybacks.to_owned(),
                    timestamp: 0, // TODO: use hlc
                })
                .await?;

            for resp in res.into_inner().piggybacks {
                match resp.info.unwrap() {
                    piggyback_response::Info::SyncRoot(_) => {}
                    piggyback_response::Info::CollectStats(_) => {
                        todo!()
                    }
                    piggyback_response::Info::CollectGroupDetail(_) => {
                        todo!()
                    }
                }
            }
        }
        Ok(())
    }
}
