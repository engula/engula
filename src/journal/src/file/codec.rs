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

use tokio::io::{
    AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt, AsyncWrite, AsyncWriteExt, SeekFrom,
};

use crate::{Error, Event, Result, Timestamp};

// Event format:
//
// | ts_len (u32) | data_len (u32) | ts_bytes (ts_len) | data_bytes (data_len) |
//
// Footer format:
//
// | ts_bytes (ts_len) | ts_len (u32) |

pub async fn write_event<W: AsyncWrite + Unpin>(w: &mut W, event: &Event) -> Result<usize> {
    let ts_bytes = event.ts.serialize();
    w.write_u32(ts_bytes.len() as u32).await?;
    w.write_u32(event.data.len() as u32).await?;
    w.write_buf(&mut ts_bytes.as_ref()).await?;
    w.write_buf(&mut event.data.as_ref()).await?;
    Ok(8 + ts_bytes.len() + event.data.len())
}

pub async fn write_footer<W: AsyncWrite + Unpin>(w: &mut W, ts: Timestamp) -> Result<usize> {
    let ts_bytes = ts.serialize();
    w.write_buf(&mut ts_bytes.as_ref()).await?;
    w.write_u32(ts_bytes.len() as u32).await?;
    Ok(ts_bytes.len() + 4)
}

pub async fn read_event_at<R: AsyncRead + Unpin>(
    r: &mut R,
    mut offset: usize,
    max_offset: usize,
) -> Result<Option<(Event, usize)>> {
    if offset == max_offset {
        return Ok(None);
    }
    offset += 8;
    if offset > max_offset {
        return Err(Error::Corrupted(format!(
            "offset {} > max_offset {}",
            offset, max_offset
        )));
    }
    let ts_len = r.read_u32().await?;
    let data_len = r.read_u32().await?;
    offset += (ts_len + data_len) as usize;
    if offset > max_offset {
        return Err(Error::Corrupted(format!(
            "offset {} > max_offset {}",
            offset, max_offset
        )));
    }
    let mut ts_buf = vec![0; ts_len as usize];
    r.read_exact(&mut ts_buf).await?;
    let ts = Timestamp::deserialize(ts_buf)?;
    let mut data = vec![0; data_len as usize];
    r.read_exact(&mut data).await?;
    Ok(Some((Event { ts, data }, offset)))
}

pub async fn read_footer_from<R: AsyncRead + AsyncSeek + Unpin>(
    r: &mut R,
    mut max_offset: usize,
) -> Result<(Timestamp, usize)> {
    if max_offset < 4 {
        return Err(Error::Corrupted("file size too small".to_owned()));
    }
    max_offset -= 4;
    r.seek(SeekFrom::Start(max_offset as u64)).await?;
    let ts_len = r.read_u32().await?;
    if max_offset < ts_len as usize {
        return Err(Error::Corrupted("file size too small".to_owned()));
    }
    max_offset -= ts_len as usize;
    r.seek(SeekFrom::Start(max_offset as u64)).await?;
    let mut ts_buf = vec![0; ts_len as usize];
    r.read_exact(&mut ts_buf).await?;
    let ts = Timestamp::deserialize(ts_buf)?;
    Ok((ts, max_offset))
}
