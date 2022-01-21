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

use std::cmp::Ordering;

use bytes::{Buf, BufMut};
use serde::{Deserialize, Serialize};

use crate::{Error, Result};

pub type Timestamp = u64;
pub const MAX_TIMESTAMP: u64 = u64::MAX;

pub type Value = Option<Vec<u8>>;

#[derive(Eq, PartialEq, Clone, Debug)]
#[repr(u8)]
pub enum ValueKind {
    None = 0,
    Some = 1,
    Unknown = 255,
}

impl From<ValueKind> for u8 {
    fn from(v: ValueKind) -> Self {
        v as u8
    }
}

impl From<u8> for ValueKind {
    fn from(v: u8) -> Self {
        match v {
            0 => ValueKind::None,
            1 => ValueKind::Some,
            _ => ValueKind::Unknown,
        }
    }
}

#[derive(Eq, PartialEq, Clone)]
pub struct InternalKey(Vec<u8>);

impl InternalKey {
    pub fn new(buf: Vec<u8>) -> Self {
        Self(buf)
    }

    pub fn for_lookup(user_key: &[u8], timestamp: Timestamp) -> Self {
        let pk = ParsedInternalKey {
            user_key,
            timestamp,
            value_kind: ValueKind::None,
        };
        pk.into()
    }

    pub fn parse(&self) -> ParsedInternalKey {
        ParsedInternalKey::decode_from(&self.0)
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl Ord for InternalKey {
    fn cmp(&self, other: &Self) -> Ordering {
        let k1 = ParsedInternalKey::decode_from(&self.0);
        let k2 = ParsedInternalKey::decode_from(&other.0);
        k1.cmp(&k2)
    }
}

impl PartialOrd for InternalKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<'a> From<ParsedInternalKey<'a>> for InternalKey {
    fn from(pk: ParsedInternalKey) -> Self {
        Self(pk.encode_to_vec())
    }
}

#[derive(Eq, Debug)]
pub struct ParsedInternalKey<'a> {
    pub user_key: &'a [u8],
    pub timestamp: Timestamp,
    pub value_kind: ValueKind,
}

impl<'a> ParsedInternalKey<'a> {
    pub fn encode_to_vec(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(self.user_key.len() + 9);
        buf.put_slice(self.user_key);
        buf.put_u64(self.timestamp);
        buf.put_u8(self.value_kind.clone().into());
        buf
    }

    pub fn decode_from(buf: &[u8]) -> ParsedInternalKey {
        let (user_key, buf) = buf.split_at(buf.len() - 9);
        let (ts, kind) = buf.split_at(8);
        let timestamp = u64::from_be_bytes(ts.try_into().unwrap());
        let value_kind = ValueKind::from(kind[0]);
        ParsedInternalKey {
            user_key,
            timestamp,
            value_kind,
        }
    }
}

impl<'a> Ord for ParsedInternalKey<'a> {
    fn cmp(&self, other: &Self) -> Ordering {
        let mut ord = self.user_key.cmp(other.user_key);
        if ord == Ordering::Equal {
            ord = match self.timestamp.cmp(&other.timestamp) {
                Ordering::Greater => Ordering::Less,
                Ordering::Less => Ordering::Greater,
                Ordering::Equal => Ordering::Equal,
            };
        }
        ord
    }
}

impl<'a> PartialOrd for ParsedInternalKey<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<'a> PartialEq for ParsedInternalKey<'a> {
    fn eq(&self, other: &Self) -> bool {
        self.user_key == other.user_key && self.timestamp == other.timestamp
    }
}

pub trait Comparator: Clone + Send + Sync + 'static {
    fn cmp(&self, lhs: &[u8], rhs: &[u8]) -> Ordering;
}

#[derive(Clone)]
pub struct BytewiseComparator;

impl Comparator for BytewiseComparator {
    fn cmp(&self, lhs: &[u8], rhs: &[u8]) -> Ordering {
        lhs.cmp(rhs)
    }
}

#[derive(Clone)]
pub struct InternalComparator;

impl Comparator for InternalComparator {
    fn cmp(&self, lhs: &[u8], rhs: &[u8]) -> Ordering {
        let lpk = ParsedInternalKey::decode_from(lhs);
        let rpk = ParsedInternalKey::decode_from(rhs);
        lpk.cmp(&rpk)
    }
}

pub fn put_timestamp(buf: &mut impl BufMut, ts: Timestamp) {
    buf.put_u64(ts);
}

pub fn decode_timestamp(mut buf: &[u8]) -> Result<(Timestamp, &[u8])> {
    if buf.len() >= std::mem::size_of::<u64>() {
        let ts = buf.get_u64();
        Ok((ts, buf))
    } else {
        Err(Error::Corrupted("invalid buffer".to_owned()))
    }
}

pub fn put_length_prefixed_slice(buf: &mut impl BufMut, s: &[u8]) {
    buf.put_u64(s.len() as u64);
    buf.put_slice(s);
}

pub fn decode_length_prefixed_slice(mut buf: &[u8]) -> Result<(&[u8], &[u8])> {
    if buf.len() >= std::mem::size_of::<u64>() {
        let len = buf.get_u64() as usize;
        if buf.len() >= len {
            return Ok(buf.split_at(len));
        }
    }
    Err(Error::Corrupted("invalid buffer".to_owned()))
}

pub fn put_value(buf: &mut impl BufMut, value: &Value) {
    match value {
        Some(value) => {
            buf.put_u8(ValueKind::Some.into());
            put_length_prefixed_slice(buf, value);
        }
        None => {
            buf.put_u8(ValueKind::None.into());
        }
    }
}

pub fn decode_value(mut buf: &[u8]) -> Result<(Value, &[u8])> {
    if !buf.is_empty() {
        let kind = ValueKind::from(buf.get_u8());
        match kind {
            ValueKind::Some => {
                let (value, buf) = decode_length_prefixed_slice(buf)?;
                return Ok((Some(value.to_owned()), buf));
            }
            ValueKind::None => return Ok((None, buf)),
            _ => {}
        }
    }
    Err(Error::Corrupted("invalid buffer".to_owned()))
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct FlushDesc {
    pub memtable_id: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum UpdateDesc {
    Flush(FlushDesc),
}

impl UpdateDesc {
    pub fn encode_to_vec(&self) -> Vec<u8> {
        let buf = serde_json::to_string(self).unwrap();
        buf.into()
    }

    pub fn decode_from(buf: &[u8]) -> Result<Self> {
        let desc: Self =
            serde_json::from_slice(buf).map_err(|x| Error::Corrupted(x.to_string()))?;
        Ok(desc)
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct TableDesc {
    pub table_size: u64,
    pub lower_bound: Vec<u8>,
    pub upper_bound: Vec<u8>,
}

impl TableDesc {
    pub fn encode_to_vec(&self) -> Vec<u8> {
        let buf = serde_json::to_string(self).unwrap();
        buf.into()
    }

    pub fn decode_from(buf: &[u8]) -> Result<Self> {
        let desc: Self =
            serde_json::from_slice(buf).map_err(|x| Error::Corrupted(x.to_string()))?;
        Ok(desc)
    }
}
