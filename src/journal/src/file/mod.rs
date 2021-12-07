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

mod error;
mod journal;
mod stream;

pub use self::{journal::Journal, stream::Stream};

#[cfg(test)]
mod tests {

    use crate::*;
    use futures::TryStreamExt;
    use tokio_stream::StreamExt;


    #[tokio::test]
    async fn simple() -> std::result::Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;

        let journal = file::Journal::new(tmp).await?;
        let stream = journal.create_stream("s").await?;
        let ts = 31340128116183;
        let event = Event {
            ts: ts.into(),
            data: vec![0, 1, 2],
        };
        stream.append_event(event.clone()).await?;
        {
            let mut events = stream.read_events(0.into()).await;
            let got = events.try_next().await?.unwrap();
            assert_eq!(got, vec![event]);
        }
        stream.release_events((ts + 1).into()).await?;
        {
            let mut events = stream.read_events(0.into()).await;
            let got = events.try_next().await?.unwrap();
            assert_eq!(got, vec![]);
        }
        let _ = journal.delete_stream("s").await?;
        Ok(())
    }

    #[tokio::test]
    async fn big() -> std::result::Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;

        let journal = file::Journal::new(tmp).await?;
        let stream = journal.create_stream("s").await?;
        let ts = 31340128116183;
        let event = Event {
            ts: ts.into(),
            data: vec![0, 1, 2],
        };
        stream.append_event(event.clone()).await?;
        {
            let mut events = stream.read_events(0.into()).await;
            let got = events.try_next().await?.unwrap();
            assert_eq!(got, vec![event]);
        }
        stream.release_events((ts + 1).into()).await?;
        {
            let mut events = stream.read_events(0.into()).await;
            let got = events.try_next().await?.unwrap();
            assert_eq!(got, vec![]);
        }
        let _ = journal.delete_stream("s").await?;
        Ok(())
    }

    #[tokio::test]
    async fn recover() -> std::result::Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;

        let journal = file::Journal::new(tmp).await?;
        let stream = journal.create_stream("s").await?;
        let ts = 31340128116183;
        let event = Event {
            ts: ts.into(),
            data: vec![0, 1, 2],
        };
        stream.append_event(event.clone()).await?;
        {
            let mut events = stream.read_events(0.into()).await;
            let got = events.try_next().await?.unwrap();
            assert_eq!(got, vec![event]);
        }
        stream.release_events((ts + 1).into()).await?;
        {
            let mut events = stream.read_events(0.into()).await;
            let got = events.try_next().await?.unwrap();
            assert_eq!(got, vec![]);
        }
        let _ = journal.delete_stream("s").await?;
        Ok(())
    }
}

