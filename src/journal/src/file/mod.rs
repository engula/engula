// Copyright 2021 The Engula Authors.
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

mod journal;
mod segment;
mod segment_reader;
mod segment_stream;
mod stream;

pub use self::{journal::Journal, stream::Stream};

#[cfg(test)]
mod tests {
    use futures::TryStreamExt;

    use crate::*;

    #[tokio::test]
    async fn test() -> Result<()> {
        let tmp = tempfile::tempdir()?;
        let root = tmp.path();
        let stream_name = "stream";
        let segment_size = 1024;

        // Creates a stream
        let j = super::Journal::open(root, segment_size).await?;
        assert!(matches!(
            j.stream(stream_name).await,
            Err(Error::NotFound(_))
        ));
        let stream = j.create_stream(stream_name).await?;
        test_stream(&stream, 1, 256).await?;

        // Reopen
        let j = super::Journal::open(root, segment_size).await?;
        assert!(matches!(
            j.create_stream(stream_name).await,
            Err(Error::AlreadyExists(_))
        ));
        let stream = j.stream(stream_name).await?;
        // This will conflict with the last timestamp.
        assert!(matches!(
            test_stream(&stream, 255, 256).await,
            Err(Error::InvalidArgument(_))
        ));
        test_stream(&stream, 256, 512).await?;

        // Deletes a stream
        j.delete_stream(stream_name).await?;
        assert!(matches!(
            j.stream(stream_name).await,
            Err(Error::NotFound(_))
        ));

        Ok(())
    }

    async fn test_stream(stream: &super::Stream, start: u64, limit: u64) -> Result<()> {
        let mut released_ts = start;
        for ts in start..limit {
            let event = Event {
                ts: ts.into(),
                data: ts.to_be_bytes().to_vec(),
            };
            stream.append_event(event).await?;
            check_stream(stream, released_ts, ts + 1).await?;
            if ts % 29 == 0 {
                released_ts = ts - 17;
                stream.release_events(released_ts.into()).await?;
                check_stream(stream, released_ts, ts + 1).await?;
            }
        }
        Ok(())
    }

    async fn check_stream(stream: &super::Stream, start: u64, limit: u64) -> Result<()> {
        let mut events = stream.read_events(start.into()).await;
        for i in start..limit {
            for event in events.try_next().await?.unwrap() {
                assert_eq!(event.ts, i.into());
            }
        }
        Ok(())
    }
}
