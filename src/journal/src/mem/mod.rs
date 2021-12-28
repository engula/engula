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
    use crate::*;

    #[tokio::test]
    async fn test() -> Result<()> {
        let stream_name = "stream";
        let j = mem::Journal::default();
        j.create_stream(stream_name).await?;

        let mut writer = j.new_stream_writer(stream_name).await?;
        let event1 = Event {
            ts: 1,
            data: vec![1],
        };
        writer.append(event1.clone()).await?;
        let event2 = Event {
            ts: 2,
            data: vec![2],
        };
        writer.append(event2.clone()).await?;

        let mut reader = j.new_stream_reader(stream_name).await?;
        reader.seek(1).await?;
        assert_eq!(reader.next().await?.as_ref(), Some(&event1));
        assert_eq!(reader.next().await?.as_ref(), Some(&event2));
        reader.seek(2).await?;
        assert_eq!(reader.next().await?.as_ref(), Some(&event2));
        Ok(())
    }
}
