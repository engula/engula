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

use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
};

use engula_kernel::Kernel;

use crate::{
    memtable::Memtable,
    version::{Scanner, Version},
    Options, ReadOptions, Result, WriteBatch,
};

pub struct Store<K: Kernel> {
    inner: Mutex<Inner>,
    _kernel: Arc<K>,
    options: Options,
}

impl<K: Kernel> Store<K> {
    pub fn new(options: Options, kernel: Arc<K>) -> Self {
        let mem = Arc::new(Memtable::new(0));
        let current = Arc::new(Version::default());
        let mut vset = VecDeque::new();
        vset.push_back(current);
        let inner = Inner { mem, vset };
        Self {
            inner: Mutex::new(inner),
            _kernel: kernel,
            options,
        }
    }

    pub fn get(&self, options: &ReadOptions, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let inner = self.inner.lock().unwrap();
        Ok(inner.mem.get(options.snapshot.ts, key))
    }

    pub fn scan(&self, _options: &ReadOptions) -> Scanner {
        let inner = self.inner.lock().unwrap();
        inner.vset.back().unwrap().scan()
    }

    pub fn write(&self, batch: WriteBatch) {
        let mut inner = self.inner.lock().unwrap();
        inner.mem.write(batch);
        if inner.mem.estimated_size() >= self.options.memtable_size {
            inner.switch_memtable();
        }
    }
}

struct Inner {
    mem: Arc<Memtable>,
    vset: VecDeque<Arc<Version>>,
}

impl Inner {
    pub fn clone_current(&self) -> Version {
        (**self.vset.back().unwrap()).clone()
    }

    pub fn switch_memtable(&mut self) {
        let mut version = self.clone_current();
        version.mem.tables.push(self.mem.clone());
        let last_ts = self.mem.last_timestamp();
        self.mem = Arc::new(Memtable::new(last_ts));
    }
}
