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
mod stream;

pub use self::journal::MemJournal;

#[cfg(test)]
mod tests {
    use futures::StreamExt;

    use super::*;
    use crate::*;

    #[tokio::test]
    async fn test() -> Result<()> {
        let j = MemJournal::default();
        let stream = j.create_stream("a").await?;
        let event = Event {
            ts: 0.into(),
            data: vec![1, 2, 3],
        };
        stream.append_event(event.clone()).await?;
        let mut events = stream.read_events(0.into()).await;
        let got = events.next().await.unwrap()?;
        assert_eq!(got, vec![event]);
        Ok(())
    }
}
