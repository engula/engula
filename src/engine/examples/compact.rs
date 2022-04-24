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

use std::{
    collections::HashSet,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread::JoinHandle,
    time::Duration,
};

use engula_engine::{
    elements::{array::Array, BoxElement},
    objects::{BoxObject, RawString},
    *,
};
use rand::{rngs::SmallRng, Rng, SeedableRng};

// const NUM_KEYS: usize = 128 * 1024; // 1M keys

const NUM_KEYS: usize = 1 * 1024 * 1024; // 1M keys
const KEY_LEN: usize = 100;
const VALUE_LEN: usize = 924;

fn prepare_baseline_dataset(db: Db) {
    println!("step 1: prepare baseline dataset, total {} keys", NUM_KEYS);

    let mut key = [0; KEY_LEN];
    let mut value = [0; VALUE_LEN];
    for i in 0..NUM_KEYS {
        key[0..8].copy_from_slice(&i.to_le_bytes());
        value[0..8].copy_from_slice(&i.to_le_bytes());

        let mut boxed_value = BoxElement::<Array>::with_capacity(value.len());
        boxed_value.data_slice_mut().copy_from_slice(&value[..]);
        let object = BoxObject::<RawString>::with_key_value(&key[..], Some(boxed_value));
        db.insert(object);
    }
}

fn insert_and_remove_keys_randomly(
    db: Db,
    delete_key_ratio: u32,
    insert_key_ratio: u32,
) -> HashSet<usize> {
    println!(
        "step 2: insert and remove keys randomly, delete key vs insert key: {}/{}",
        delete_key_ratio, insert_key_ratio
    );

    let start = NUM_KEYS;
    let end = start + NUM_KEYS;

    let mut key = [0; KEY_LEN];
    let mut value = [0; VALUE_LEN];

    let mut deleted_keys: HashSet<usize> = HashSet::default();
    let mut select_gen = SmallRng::from_entropy();
    let mut key_gen = SmallRng::from_entropy();
    for i in start..end {
        key[0..8].copy_from_slice(&i.to_le_bytes());
        value[0..8].copy_from_slice(&i.to_le_bytes());

        let mut boxed_value = BoxElement::<Array>::with_capacity(value.len());
        boxed_value.data_slice_mut().copy_from_slice(&value[..]);
        let object = BoxObject::<RawString>::with_key_value(&key[..], Some(boxed_value));
        db.insert(object);

        if select_gen.gen_ratio(delete_key_ratio, insert_key_ratio) {
            let deleted_index = key_gen.gen_range(0..i);
            key[0..8].copy_from_slice(&deleted_index.to_le_bytes());
            db.remove(&key[..]);
            deleted_keys.insert(deleted_index);
        }

        // if i % 100 == 0 {
        //     unsafe {
        //         compact_segments(|record_base| migrate_record(db.clone(), record_base));
        //     }
        // }
    }

    // unsafe {
    //     compact_segments(|record_base| migrate_record(db.clone(), record_base));
    // }

    deleted_keys
}

fn validate_existed_keys(db: Db, deleted_keys: HashSet<usize>) {
    println!("step 3: validate existed keys");

    let mut key = [0; KEY_LEN];
    let mut value = [0; VALUE_LEN];

    let end = NUM_KEYS * 2;
    for i in 0..end {
        if deleted_keys.contains(&i) {
            continue;
        }
        key[0..8].copy_from_slice(&i.to_le_bytes());
        value[0..8].copy_from_slice(&i.to_le_bytes());

        match db.get(&key) {
            Some(raw_object) => {
                let object = raw_object
                    .data::<RawString>()
                    .expect("value should be RawString");
                assert_eq!(object.data_slice(), &value[..]);
            }
            None => {
                panic!("key with index {} is lost", i);
            }
        }
    }
}

fn report_memory_stats(num_deleted_keys: usize) {
    let ratio = |a, b| (a as f64 / b as f64);
    let mb = |v| v / 1024 / 1024;

    let total_keys = NUM_KEYS * 2;
    println!("");
    println!("total keys: {}", total_keys);
    println!("deleted keys: {}", num_deleted_keys);
    println!("deleted ratio: {}", ratio(num_deleted_keys, total_keys));

    let live_keys = total_keys - num_deleted_keys;
    let memory = live_keys * (KEY_LEN + VALUE_LEN + 8); // variant length.
    let touched_memory = total_keys * (KEY_LEN + VALUE_LEN + 8); // variant length.
    println!("conceptual used memory: {}MB", mb(memory));
    println!("conceptual touched memory: {}MB", mb(touched_memory));

    let stats = read_mem_stats();
    println!("");
    println!("lsa used segments:");
    println!("consumed: {}MB", mb(stats.total));
    println!("allocated: {}MB", mb(stats.allocated));
    println!("freed: {}MB", mb(stats.freed));
    println!("freed ratio: {}", ratio(stats.freed, stats.total));
    println!("lsa freed segment: {}", stats.freed_segments);
    println!("num compaction: {}", stats.compacted_segments);
}

use std::sync::{Condvar, Mutex};

use lazy_static::lazy_static;

lazy_static! {
    static ref MUTEX: Mutex<()> = Mutex::new(());
    static ref COND_VAR: Condvar = Condvar::new();
}

fn waker() {
    let _guard = MUTEX.lock().unwrap();
    COND_VAR.notify_all();
}

fn wait() {
    let guard = MUTEX.lock().unwrap();
    unsafe { engula_engine::alloc::wait_for_compaction(waker) };
    let _guard = COND_VAR.wait(guard);
}

fn run_background(db: Db, exit_flags: Arc<AtomicBool>) {
    while !exit_flags.load(Ordering::Acquire) {
        wait();
        loop {
            unsafe {
                if !compact_segments(|record_base| migrate_record(db.clone(), record_base)) {
                    break;
                }
            }
        }
    }
}

fn bootstrap_background_service(db: Db) -> (Arc<AtomicBool>, JoinHandle<()>) {
    let exit_flag = Arc::new(AtomicBool::new(false));
    let cloned_exit_flag = exit_flag.clone();
    let join_handle = std::thread::spawn(move || {
        run_background(db, cloned_exit_flag);
    });
    (exit_flag, join_handle)
}

fn main() {
    let db = Db::default();

    let (exit_flag, join_handle) = bootstrap_background_service(db.clone());

    prepare_baseline_dataset(db.clone());
    let deleted_keys = insert_and_remove_keys_randomly(db.clone(), 10, 10);
    let num_deleted_keys = deleted_keys.len();
    validate_existed_keys(db.clone(), deleted_keys);

    unsafe { engula_engine::alloc::wake_compaction() };
    std::thread::sleep(Duration::from_millis(100));

    exit_flag.store(true, Ordering::Release);
    waker();
    join_handle.join().unwrap();

    report_memory_stats(num_deleted_keys);

    println!("success");
}
