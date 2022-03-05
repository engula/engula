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

use std::{cmp::Ordering, mem::size_of, ops::Deref};

use bytes::BufMut;

pub type Timestamp = u64;

#[derive(Eq, Copy, Clone)]
pub struct Key<'a>(&'a [u8]);

impl<'a> Key<'a> {
    pub fn id(&self) -> &[u8] {
        &self.0[..(self.0.len() - 9)]
    }

    pub fn ts(&self) -> Timestamp {
        let len = self.0.len();
        let range = (len - 9)..(len - 1);
        Timestamp::from_le_bytes(self.0[range].try_into().unwrap())
    }

    pub fn tp(&self) -> ValueType {
        (*self.0.last().unwrap()).try_into().unwrap()
    }

    pub fn as_slice(&self) -> &[u8] {
        self.0
    }

    pub fn to_owned(&self) -> Vec<u8> {
        self.0.to_owned()
    }

    pub fn encode_to(buf: &mut impl BufMut, id: &[u8], ts: Timestamp, tp: ValueType) {
        buf.put_slice(id);
        buf.put_u64_le(ts);
        buf.put_u8(tp.into());
    }

    pub fn encode_to_vec(id: &[u8], ts: Timestamp, tp: ValueType) -> Vec<u8> {
        let len = id.len() + size_of::<Timestamp>() + 1;
        let mut buf = Vec::with_capacity(len);
        Self::encode_to(&mut buf, id, ts, tp);
        buf
    }
}

impl<'a> From<&'a [u8]> for Key<'a> {
    fn from(v: &'a [u8]) -> Self {
        Self(v)
    }
}

impl<'a> Deref for Key<'a> {
    type Target = [u8];

    fn deref(&self) -> &'a Self::Target {
        self.0
    }
}

impl<'a> Ord for Key<'a> {
    fn cmp(&self, other: &Self) -> Ordering {
        let mut ord = self.id().cmp(other.id());
        if ord == Ordering::Equal {
            ord = self.ts().cmp(&other.ts()).reverse();
        }
        ord
    }
}

impl<'a> PartialOrd for Key<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<'a> PartialEq for Key<'a> {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

pub enum ValueType {
    Put = 0,
    Merge = 1,
    Delete = 2,
}

impl From<ValueType> for u8 {
    fn from(v: ValueType) -> Self {
        match v {
            ValueType::Put => 0,
            ValueType::Merge => 1,
            ValueType::Delete => 2,
        }
    }
}

impl TryFrom<u8> for ValueType {
    type Error = u8;

    fn try_from(v: u8) -> Result<Self, Self::Error> {
        match v {
            0 => Ok(Self::Put),
            1 => Ok(Self::Merge),
            2 => Ok(Self::Delete),
            _ => Err(v),
        }
    }
}
