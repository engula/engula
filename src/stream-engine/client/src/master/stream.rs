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

use stream_engine_proto::*;

use super::MasterClient;
use crate::{Error, Result, Sequence};

#[derive(Debug, Clone)]
pub struct ObserverMeta {
    pub observer_id: String,
    pub state: ObserverState,

    /// The value of epoch in observer's memory.
    pub writer_epoch: u32,

    /// The acked sequence of entries it has already known. It might less than
    /// (epoch << 32).
    pub acked_seq: Sequence,
}

#[derive(Clone)]
pub struct Stream {
    tenant: String,
    stream_desc: StreamDesc,
    master_client: MasterClient,
}

#[allow(unused)]
impl Stream {
    pub(super) fn new(
        tenant: String,
        stream_desc: StreamDesc,
        master_client: MasterClient,
    ) -> Self {
        Stream {
            tenant,
            stream_desc,
            master_client,
        }
    }

    #[inline(always)]
    pub fn desc(&self) -> StreamDesc {
        self.stream_desc.clone()
    }

    #[inline(always)]
    pub fn stream_id(&self) -> u64 {
        self.stream_desc.id
    }

    /// Sends the state of a stream observer to master, and receives commands.
    pub async fn heartbeat(&self, observer_meta: ObserverMeta) -> Result<Vec<Command>> {
        let role: Role = observer_meta.state.into();
        let req = HeartbeatRequest {
            tenant: self.tenant.clone(),
            writer_epoch: observer_meta.writer_epoch,
            observer_id: observer_meta.observer_id,
            stream_id: self.stream_desc.id,
            role: role as i32,
            observer_state: observer_meta.state as i32,
            acked_seq: observer_meta.acked_seq.into(),
        };

        let resp = self.master_client.heartbeat(req).await?;
        Ok(resp.commands.into_iter().map(Into::into).collect())
    }

    /// Get segment desc of the specified epoch of a stream.
    pub async fn get_segment(&self, epoch: u32) -> Result<Option<SegmentDesc>> {
        self.get_segments(vec![epoch])
            .await?
            .first()
            .cloned()
            .ok_or(Error::InvalidResponse)
    }

    pub async fn get_segments(&self, epochs: Vec<u32>) -> Result<Vec<Option<SegmentDesc>>> {
        type Request = segment_request_union::Request;
        type Response = segment_response_union::Response;

        let sub_requests = epochs
            .into_iter()
            .map(|epoch| SegmentRequestUnion {
                request: Some(Request::GetSegment(GetSegmentRequest {
                    segment_epoch: epoch,
                })),
            })
            .collect();

        let req = SegmentRequest {
            tenant: self.tenant.clone(),
            stream_id: self.stream_desc.id,
            requests: sub_requests,
        };
        let resp = self.master_client.segment(req).await?;
        resp.responses
            .into_iter()
            .map(|resp| match resp.response {
                Some(Response::GetSegment(resp)) => Ok(resp.desc),
                _ => Err(Error::InvalidResponse),
            })
            .collect()
    }

    /// Mark the corresponding segment as sealed.  The request is ignored if the
    /// target segment is already sealed.
    pub async fn seal_segment(&self, epoch: u32) -> Result<()> {
        let req = segment_request_union::Request::SealSegment(SealSegmentRequest {
            segment_epoch: epoch,
        });
        self.master_client
            .segment_union(self.tenant.clone(), self.stream_desc.id, req)
            .await?;
        Ok(())
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use stream_engine_master::build_master;

    use super::*;
    use crate::{master::Master, Sequence};

    #[tokio::test(flavor = "multi_thread")]
    async fn heartbeat() -> Result<()> {
        let replicas = vec!["a", "b", "c"];
        let local_addr = build_master(&replicas).await?;
        let master = Master::new(&local_addr.to_string()).await?;
        let tenant = master.create_tenant("tenant").await?;
        let stream = tenant.create_stream_client("stream").await?;

        let observer_meta = ObserverMeta {
            observer_id: "1".to_owned(),
            writer_epoch: 1,
            state: ObserverState::Leading,
            acked_seq: Sequence::new(1, 0),
        };
        stream.heartbeat(observer_meta).await?;

        Ok(())
    }

    async fn default_heartbeat(stream: &crate::master::stream::Stream) -> Result<()> {
        let observer_meta = ObserverMeta {
            observer_id: "1".to_owned(),
            writer_epoch: 0,
            state: ObserverState::Following,
            acked_seq: Sequence::new(0, 0),
        };
        stream.heartbeat(observer_meta).await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn get_segment() -> Result<()> {
        let replicas = vec!["a", "b", "c"];
        let local_addr = build_master(&replicas).await?;
        let master = Master::new(&local_addr.to_string()).await?;
        let tenant = master.create_tenant("tenant").await?;
        let stream = tenant.create_stream_client("stream").await?;
        default_heartbeat(&stream).await?;

        let resp = stream.get_segment(1).await?;
        assert!(
            matches!(resp, Some(segment_desc) if segment_desc == SegmentDesc {
                stream_id: 1,
                epoch: 1,
                copy_set: replicas.iter().map(ToString::to_string).collect(),
                state: SegmentState::Appending as i32,
            })
        );

        let resp = stream.get_segment(2).await?;
        assert!(matches!(resp, None));

        Ok(())
    }

    fn expect_promote_command(cmd: &Command, target_role: Role, target_epoch: u32) -> bool {
        matches!(cmd, Command { command_type, role, epoch, .. }
            if *command_type == CommandType::Promote as i32 &&
                *role == target_role as i32 &&
                *epoch == target_epoch)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn heartbeat_with_threshold_switching() -> Result<()> {
        let replicas = vec!["a", "b", "c"];
        let local_addr = build_master(&replicas).await?;
        let master = Master::new(&local_addr.to_string()).await?;
        let tenant = master.create_tenant("tenant").await?;
        let stream = tenant.create_stream_client("stream").await?;

        let observer_meta = ObserverMeta {
            observer_id: "1".to_owned(),
            writer_epoch: 0,
            state: ObserverState::Leading,
            acked_seq: u64::MAX.into(),
        };

        let commands = stream.heartbeat(observer_meta).await?;
        assert_eq!(commands.len(), 1);
        assert!(expect_promote_command(&commands[0], Role::Leader, 1));
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn heartbeat_promote_leader_or_followers() -> Result<()> {
        let replicas = vec!["a", "b", "c"];
        let local_addr = build_master(&replicas).await?;
        let master = Master::new(&local_addr.to_string()).await?;
        let tenant = master.create_tenant("tenant").await?;
        let stream = tenant.create_stream_client("stream").await?;

        let observer_meta = ObserverMeta {
            observer_id: "1".to_owned(),
            writer_epoch: 0,
            state: ObserverState::Following,
            acked_seq: Sequence::new(1, 0),
        };
        let commands = stream.heartbeat(observer_meta).await?;
        println!("commands {:?}", commands);
        let promote = commands
            .iter()
            .find(|cmd| expect_promote_command(cmd, Role::Leader, 1));
        assert!(promote.is_some());

        // Now a follower send heartbeat request and receive promote request.
        let observer_meta = ObserverMeta {
            observer_id: "2".to_owned(),
            writer_epoch: 0,
            state: ObserverState::Following,
            acked_seq: Sequence::new(1, 0),
        };
        let commands = stream.heartbeat(observer_meta).await?;
        let promote = commands
            .iter()
            .find(|cmd| expect_promote_command(cmd, Role::Follower, 1));
        assert!(promote.is_some());
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn seal_segment() -> Result<()> {
        let master_addr = build_master(&[]).await?;
        let master = Master::new(&master_addr).await?;
        let tenant = master.create_tenant("tenant").await?;
        let stream = tenant.create_stream_client("stream").await?;
        default_heartbeat(&stream).await?;
        let meta = stream.get_segment(1).await?.unwrap();
        assert_eq!(meta.state, SegmentState::Appending as i32);

        stream.seal_segment(meta.epoch).await?;
        let meta = stream.get_segment(1).await?.unwrap();
        assert_eq!(meta.state, SegmentState::Sealed as i32);

        Ok(())
    }

    /// If observer lost the heartbeat response, it should receive and continue
    /// the previous promote request.
    #[tokio::test(flavor = "multi_thread")]
    async fn heartbeat_idempotent() -> std::result::Result<(), Box<dyn std::error::Error>> {
        let replicas = vec!["a", "b", "c"];
        let local_addr = build_master(&replicas).await?;
        let master = Master::new(&local_addr.to_string()).await?;
        let tenant = master.create_tenant("tenant").await?;
        let stream = tenant.create_stream_client("stream").await?;
        let observer_meta = ObserverMeta {
            observer_id: "1".to_owned(),
            writer_epoch: 0,
            state: ObserverState::Following,
            acked_seq: Sequence::new(1, 0),
        };
        let commands = stream.heartbeat(observer_meta).await?;
        let prev_promote = commands
            .iter()
            .find(|cmd| expect_promote_command(cmd, Role::Leader, 1))
            .unwrap();
        let observer_meta = ObserverMeta {
            observer_id: "1".to_owned(),
            writer_epoch: 0,
            state: ObserverState::Following,
            acked_seq: Sequence::new(1, 0),
        };
        let commands = stream.heartbeat(observer_meta).await?;
        let new_promote = commands
            .iter()
            .find(|cmd| expect_promote_command(cmd, Role::Leader, 1))
            .unwrap();
        match (prev_promote, new_promote) {
            (
                Command {
                    epoch: prev_epoch, ..
                },
                Command {
                    epoch: new_epoch, ..
                },
            ) if prev_epoch == new_epoch => {}
            _ => panic!("shouldn't happen"),
        }
        Ok(())
    }
}
