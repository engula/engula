use std::{sync::Arc, time::Instant};

use clap::Clap;
use engula::*;
use tokio::{runtime, sync::Barrier};

use super::config::Config;

#[derive(Clap, Debug)]
pub struct Command {
    #[clap(long)]
    get: bool,
}

impl Command {
    pub fn run(&self, config: Config) -> Result<()> {
        let num_db_cores = num_cpus::get() - config.num_cores;
        let db_rt = new_runtime(num_db_cores);
        let bench_rt = new_runtime(config.num_cores);

        let db = db_rt.block_on(config.new_db())?;
        let db_clone = db.clone();
        let config_clone = config.clone();
        bench_rt.block_on(async move {
            bench_put(db_clone, config_clone).await;
        });

        if self.get {
            bench_rt.block_on(async move {
                bench_get(db, config).await;
            });
        }

        Ok(())
    }
}

fn new_runtime(num_threads: usize) -> runtime::Runtime {
    runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(num_threads)
        .build()
        .unwrap()
}

async fn bench_get(db: Arc<Database>, config: Config) {
    let mut tasks = Vec::new();
    let barrier = Arc::new(Barrier::new(config.num_tasks));
    let num_entries_per_task = config.num_entries / config.num_tasks;

    let now = Instant::now();
    for task_id in 0..config.num_tasks {
        let db = db.clone();
        let barrier = barrier.clone();
        let start = task_id * num_entries_per_task;
        let end = (task_id + 1) * num_entries_per_task;
        let task = tokio::task::spawn(async move {
            barrier.wait().await;
            for i in start..end {
                let key = i.to_be_bytes();
                db.get(&key).await.unwrap().unwrap();
            }
        });
        tasks.push(task);
    }
    for task in tasks {
        task.await.unwrap();
    }

    let elapsed = now.elapsed();
    let qps = config.num_entries as f64 / elapsed.as_secs_f64();
    println!("elapsed: {:?}", elapsed);
    println!("qps: {}", qps);
}

async fn bench_put(db: Arc<Database>, config: Config) {
    let mut tasks = Vec::new();
    let barrier = Arc::new(Barrier::new(config.num_tasks));
    let num_entries_per_task = config.num_entries / config.num_tasks;

    let now = Instant::now();
    for task_id in 0..config.num_tasks {
        let mut value = Vec::new();
        value.resize(config.value_size, 0);
        let db = db.clone();
        let barrier = barrier.clone();
        let start = task_id * num_entries_per_task;
        let end = (task_id + 1) * num_entries_per_task;
        let task = tokio::task::spawn(async move {
            barrier.wait().await;
            for i in start..end {
                let key = i.to_be_bytes();
                db.put(key.to_vec(), value.clone()).await.unwrap();
            }
        });
        tasks.push(task);
    }
    for task in tasks {
        task.await.unwrap();
    }

    let elapsed = now.elapsed();
    let qps = config.num_entries as f64 / elapsed.as_secs_f64();
    println!("elapsed: {:?}", elapsed);
    println!("qps: {}", qps);
}
