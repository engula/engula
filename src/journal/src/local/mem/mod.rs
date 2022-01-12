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

//! A [`Journal`] implementation that stores data in memory.
//!
//! [`Journal`]: crate::Journal

mod journal;

pub use self::journal::Journal;

#[cfg(test)]
mod tests {
    use tokio::{task::JoinHandle, time::Duration};

    use crate::*;

    #[tokio::test]
    async fn try_next() -> Result<()> {
        let stream_name = "stream";

        let j = super::Journal::default();
        j.create_stream(stream_name).await?;

        let event1 = vec![1];
        let event2 = vec![2];
        let mut writer = j.new_stream_writer(stream_name).await?;

        let expected_event1_seq = 0;
        let expected_event2_seq = 1;
        assert_eq!(writer.append(event1.clone()).await?, expected_event1_seq);
        assert_eq!(writer.append(event2.clone()).await?, expected_event2_seq);

        let mut reader = j.new_stream_reader(stream_name).await?;
        reader.seek(expected_event1_seq).await?;
        assert_eq!(reader.try_next().await?.as_ref(), Some(&event1));
        assert_eq!(reader.try_next().await?.as_ref(), Some(&event2));
        reader.seek(expected_event2_seq).await?;
        assert_eq!(reader.try_next().await?.as_ref(), Some(&event2));
        Ok(())
    }

    #[tokio::test]
    async fn wait_next() -> Result<()> {
        let stream_name = "stream";

        let j = super::Journal::default();
        j.create_stream(stream_name).await?;

        let expected_event1_seq = 0;
        let expected_event2_seq = 1;
        let expected_event3_seq = 2;

        // 1. wait not available event
        let mut reader = j.new_stream_reader(stream_name).await?;
        let handle: JoinHandle<Result<()>> = tokio::spawn(async move {
            let event2 = vec![2];
            reader.seek(expected_event2_seq).await?;
            assert_eq!(&reader.wait_next().await?, &event2);
            Ok(())
        });

        tokio::time::sleep(Duration::from_millis(10)).await;

        let event1 = vec![1];
        let event2 = vec![2];
        let event3 = vec![3];
        let mut writer = j.new_stream_writer(stream_name).await?;
        assert_eq!(writer.append(event1.clone()).await?, expected_event1_seq);
        assert_eq!(writer.append(event2.clone()).await?, expected_event2_seq);

        handle.await.unwrap()?;

        // 2. wait available event
        let mut reader = j.new_stream_reader(stream_name).await?;
        reader.seek(expected_event1_seq).await?;
        assert_eq!(reader.try_next().await?.as_ref(), Some(&event1));
        assert_eq!(reader.try_next().await?.as_ref(), Some(&event2));

        assert_eq!(writer.append(event3.clone()).await?, expected_event3_seq);
        assert_eq!(&reader.wait_next().await?, &event3);

        Ok(())
    }

    #[tokio::test]
    async fn seek() -> Result<()> {
        let stream_name = "stream";

        let j = super::Journal::default();
        j.create_stream(stream_name).await?;

        let mut reader = j.new_stream_reader(stream_name).await?;

        let expected_event1_seq = 0;
        let expected_event2_seq = 1;

        // seek future sequence
        reader.seek(expected_event2_seq).await?;

        let event1 = vec![1];
        let event2 = vec![2];
        let mut writer = j.new_stream_writer(stream_name).await?;
        assert_eq!(writer.append(event1.clone()).await?, expected_event1_seq);
        assert_eq!(writer.append(event2.clone()).await?, expected_event2_seq);
        assert_eq!(reader.try_next().await?.as_ref(), Some(&event2));

        // normal
        reader.seek(expected_event1_seq).await?;
        assert_eq!(reader.try_next().await?.as_ref(), Some(&event1));
        assert_eq!(reader.try_next().await?.as_ref(), Some(&event2));
        reader.seek(expected_event2_seq).await?;
        assert_eq!(reader.try_next().await?.as_ref(), Some(&event2));

        // cannot seek truncated sequence
        writer.truncate(expected_event2_seq + 1).await?;
        let got = reader.seek(expected_event2_seq).await;
        assert!(got.is_err());

        Ok(())
    }

    #[tokio::test]
    async fn truncate() -> Result<()> {
        let stream_name = "stream";

        let j = super::Journal::default();
        j.create_stream(stream_name).await?;

        let event1 = vec![1];
        let event2 = vec![2];
        let event3 = vec![3];

        let expected_event1_seq = 0;
        let expected_event2_seq = 1;
        let expected_event3_seq = 2;

        let mut writer = j.new_stream_writer(stream_name).await?;
        assert_eq!(writer.append(event1.clone()).await?, expected_event1_seq);
        assert_eq!(writer.append(event2.clone()).await?, expected_event2_seq);
        assert_eq!(writer.append(event3.clone()).await?, expected_event3_seq);

        // normal
        let mut reader = j.new_stream_reader(stream_name).await?;
        reader.seek(expected_event1_seq).await?;
        assert_eq!(reader.try_next().await?.as_ref(), Some(&event1));
        assert_eq!(reader.try_next().await?.as_ref(), Some(&event2));
        assert_eq!(reader.try_next().await?.as_ref(), Some(&event3));
        reader.seek(expected_event2_seq).await?;
        assert_eq!(reader.try_next().await?.as_ref(), Some(&event2));
        assert_eq!(reader.try_next().await?.as_ref(), Some(&event3));

        // truncate
        writer.truncate(expected_event2_seq).await?;
        reader.seek(expected_event2_seq).await?;
        assert_eq!(reader.try_next().await?.as_ref(), Some(&event2));
        assert_eq!(reader.try_next().await?.as_ref(), Some(&event3));
        reader.seek(expected_event3_seq).await?;
        assert_eq!(reader.try_next().await?.as_ref(), Some(&event3));

        // truncate more
        writer.truncate(expected_event3_seq).await?;
        reader.seek(expected_event3_seq).await?;
        assert_eq!(reader.try_next().await?.as_ref(), Some(&event3));

        // truncate truncated sequence is valid as noop
        writer.truncate(expected_event3_seq - 1).await?;
        reader.seek(expected_event3_seq).await?;
        assert_eq!(reader.try_next().await?.as_ref(), Some(&event3));
        writer.truncate(expected_event3_seq).await?;
        reader.seek(expected_event3_seq).await?;
        assert_eq!(reader.try_next().await?.as_ref(), Some(&event3));

        // cannot truncate future sequence
        let got = writer.truncate(expected_event3_seq + 2).await;
        assert!(got.is_err());

        Ok(())
    }
}
