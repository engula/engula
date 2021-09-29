use std::{sync::Arc, time::Instant};

use clap::Clap;
use engula::*;
use tokio::sync::Barrier;

use super::config::Config;

#[derive(Clap, Debug)]
pub struct Command {
    #[clap(long)]
    put: bool,
    #[clap(long)]
    get: bool,
}

impl Command {
    pub async fn run(&self, config: Config) -> Result<()> {
        if self.put {
            let _ = std::fs::remove_dir_all(&config.journal.path);
            let _ = std::fs::remove_dir_all(&config.storage.path);
        }

        let db = config.new_db().await?;
        if self.put {
            bench_put(db.clone(), config.clone()).await;
            db.flush().await;
        }
        if self.get {
            // Warms up
            bench_get(db.clone(), config.clone(), true).await;
            bench_get(db.clone(), config.clone(), false).await;
        }
        Ok(())
    }
}

async fn bench_put(db: Arc<Database>, config: Config) {
    let mut tasks = Vec::new();
    let barrier = Arc::new(Barrier::new(config.num_put_tasks));
    let num_entries_per_task = config.num_entries / config.num_put_tasks;

    let now = Instant::now();
    for task_id in 0..config.num_put_tasks {
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
                db.timed_put(key.to_vec(), value.clone()).await.unwrap();
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

async fn bench_get(db: Arc<Database>, config: Config, warmup: bool) {
    let mut tasks = Vec::new();
    let barrier = Arc::new(Barrier::new(config.num_get_tasks));
    let num_entries_per_task = config.num_entries / config.num_get_tasks;

    let now = Instant::now();
    for task_id in 0..config.num_get_tasks {
        let db = db.clone();
        let barrier = barrier.clone();
        let start = task_id * num_entries_per_task;
        let end = (task_id + 1) * num_entries_per_task;
        let task = tokio::task::spawn(async move {
            barrier.wait().await;
            if warmup {
                for i in (start..end).step_by(10) {
                    let key = i.to_be_bytes();
                    db.get(&key).await.unwrap().unwrap();
                }
            } else {
                for i in start..end {
                    let key = i.to_be_bytes();
                    db.timed_get(&key).await.unwrap().unwrap();
                }
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
