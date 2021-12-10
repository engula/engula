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

use crate::{file::segment::Index, Error, Event, Result, Timestamp};

const SIZE_LENGTH: u64 = 8;
const TIME_LENGTH: u64 = 8;
const HEADER_LENGTH: u64 = SIZE_LENGTH + TIME_LENGTH;

pub struct Codec;

impl Codec {
    pub fn decode_index(buf: &[u8]) -> Result<Vec<Index>> {
        let buf_size = buf.len() as u64;
        let mut ret = Vec::new();
        let mut i = 0_u64;
        loop {
            if i >= buf_size {
                break;
            }

            if i + SIZE_LENGTH > buf_size {
                return Err(Error::Unknown("decode_index size field not found".to_string()));
            }

            let size_bytes = &buf[i as usize..(i + SIZE_LENGTH) as usize];
            let mut size_buf = [0; SIZE_LENGTH as usize];
            size_buf.clone_from_slice(size_bytes);
            let size = u64::from_be_bytes(size_buf);

            if (i + HEADER_LENGTH) > buf_size {
                return Err(Error::Unknown("decode_index timestamp field not found".to_string()));
            }

            let time_bytes = buf[(i + SIZE_LENGTH) as usize..(i + HEADER_LENGTH) as usize].to_vec();
            let time = Timestamp::deserialize(time_bytes)?;

            let index = Index {
                ts: time,
                location: i as u64,
                size: size as u64,
            };

            ret.push(index);
            i += size;
        }
        Ok(ret)
    }

    pub fn decode_event(buf: &[u8]) -> Result<Vec<Event>> {
        let buf_size = buf.len() as u64;
        let mut ret = Vec::new();
        let mut i = 0_u64;
        loop {
            if i >= buf_size {
                break;
            }

            if i + SIZE_LENGTH > buf_size {
                return Err(Error::Unknown("decode event size field not found".to_string()));
            }

            let size_bytes = &buf[i as usize..(i + SIZE_LENGTH) as usize];
            let mut size_buf = [0; SIZE_LENGTH as usize];
            size_buf.clone_from_slice(size_bytes);
            let size = u64::from_be_bytes(size_buf);

            if i + HEADER_LENGTH > buf_size {
                return Err(Error::Unknown("decode event timestamp field not found".to_string()));
            }

            let time_bytes = buf[(i + SIZE_LENGTH) as usize..(i + HEADER_LENGTH) as usize].to_vec();
            let time = Timestamp::deserialize(time_bytes)?;

            if i + size > buf_size {
                return Err(Error::Unknown("decode event data field not found".to_string()));
            }

            let data = buf[(i + HEADER_LENGTH) as usize..(i + size) as usize].to_vec();
            ret.push(Event { ts: time, data });
            i += size;
        }

        Ok(ret)
    }

    pub fn encode(mut event: Event) -> Vec<u8> {
        let mut time_bytes = event.ts.serialize();

        let total_size = SIZE_LENGTH as usize + time_bytes.len() + event.data.len();
        let mut buf = Vec::<u8>::with_capacity(total_size);

        let mut size_bytes = total_size.to_be_bytes().to_vec();
        buf.append(&mut size_bytes);
        buf.append(&mut time_bytes);
        buf.append(&mut event.data);

        buf
    }
}
