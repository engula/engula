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
    use futures::TryStreamExt;

    use crate::*;

    #[tokio::test]
    async fn simple() -> std::result::Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;

        let journal = file::Journal::new(tmp.path()).await?;
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
    async fn two_read() -> std::result::Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;

        let journal = file::Journal::new(tmp.path()).await?;
        let stream = journal.create_stream("s").await?;
        let mut ts = 31340128116183;

        let event1 = Event {
            ts: ts.into(),
            data: vec![0, 1, 2],
        };
        stream.append_event(event1.clone()).await?;

        ts += 1;
        let event2 = Event {
            ts: ts.into(),
            data: vec![3, 4, 5],
        };
        stream.append_event(event2.clone()).await?;

        {
            let mut events = stream.read_events(0.into()).await;
            let got = events.try_next().await?.unwrap();
            assert_eq!(got.len(), 2);
            assert_eq!(got[0], event1);
            assert_eq!(got[1], event2);
        }

        {
            let mut events = stream.read_events((ts).into()).await;
            let got = events.try_next().await?.unwrap();
            assert_eq!(got.len(), 1);
            assert_eq!(got[0], event2);
        }

        let _ = journal.delete_stream("s").await?;
        Ok(())
    }

    #[tokio::test]
    async fn delete_and_read() -> std::result::Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;

        let journal = file::Journal::new(tmp.path()).await?;
        let stream = journal.create_stream("s").await?;
        let mut ts = 31340128116183;

        {
            let event = Event {
                ts: ts.into(),
                data: vec![0, 1, 2],
            };
            stream.append_event(event.clone()).await?;
        }

        {
            ts += 1;
            let event = Event {
                ts: ts.into(),
                data: vec![3, 4, 5],
            };
            stream.append_event(event.clone()).await?;
            stream.release_events((ts).into()).await?;
            let mut events = stream.read_events(0.into()).await;
            let got = events.try_next().await?.unwrap();
            assert_eq!(got, vec![event]);
        }

        let _ = journal.delete_stream("s").await?;
        Ok(())
    }

    #[tokio::test]
    async fn big() -> std::result::Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;

        let journal = file::Journal::new(tmp.path()).await?;
        let stream = journal.create_stream("s").await?;
        let mut ts = 31340128116183;

        {
            let event = Event {
                ts: ts.into(),
                data: vec![1; 1024 * 1024 * 30],
            };
            stream.append_event(event.clone()).await?;
        }

        {
            ts += 1;
            let event = Event {
                ts: ts.into(),
                data: vec![2; 1024 * 1024 * 30],
            };
            stream.append_event(event.clone()).await?;
        }

        {
            let ts = ts + 1;
            let event = Event {
                ts: ts.into(),
                data: vec![3; 1024 * 1024 * 30],
            };
            stream.append_event(event.clone()).await?;

            let mut events = stream.read_events(ts.into()).await;
            let got = events.try_next().await?.unwrap();
            assert_eq!(got[0].data.len(), event.data.len());
            assert_eq!(got[0].data[1024], event.data[1024]);
            assert_eq!(got[0].data[1024 * 1024], event.data[1024 * 1024]);
            assert_eq!(got[0].data[1024 * 1024 * 29], event.data[1024 * 1024 * 29]);
            assert_eq!(got[0].ts, event.ts);
        }

        let _ = journal.delete_stream("s").await?;
        Ok(())
    }

    #[tokio::test]
    async fn recover() -> std::result::Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;

        {
            let journal = file::Journal::new(tmp.path()).await?;
            let stream = journal.create_stream("s").await?;
            let mut ts = 31340128116183;

            let event1 = Event {
                ts: ts.into(),
                data: vec![0, 1, 2],
            };
            stream.append_event(event1.clone()).await?;

            ts += 1;
            let event2 = Event {
                ts: ts.into(),
                data: vec![3, 4, 5],
            };
            stream.append_event(event2.clone()).await?;
        }

        // use two journal object to mock recover situation
        {
            let journal = file::Journal::new(tmp.path()).await?;
            let stream = journal.stream("s").await?;
            let mut ts = 31340128116183;

            let event1 = Event {
                ts: ts.into(),
                data: vec![0, 1, 2],
            };

            ts += 1;
            let event2 = Event {
                ts: ts.into(),
                data: vec![3, 4, 5],
            };

            {
                let mut events = stream.read_events(0.into()).await;
                let got = events.try_next().await?.unwrap();
                assert_eq!(got.len(), 2);
                assert_eq!(got[0], event1);
                assert_eq!(got[1], event2);
            }
            stream.release_events((ts + 1).into()).await?
        }

        // check delete file when recover
        {
            let journal = file::Journal::new(tmp.path()).await?;
            let stream = journal.stream("s").await?;
            let mut events = stream.read_events(0.into()).await;
            let got = events.try_next().await?.unwrap();
            assert_eq!(got.len(), 0);
        }

        Ok(())
    }
}
