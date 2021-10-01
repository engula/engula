use std::{sync::Arc, time::Instant};

use clap::Clap;
use engula::*;
use rand::{distributions::Alphanumeric, thread_rng, Rng};
use tokio::sync::Barrier;

use super::config::Config;

#[derive(Clap, Debug)]
pub struct Command {
    #[clap(long)]
    put: bool,
    #[clap(long)]
    get: bool,
    #[clap(long)]
    count: bool,
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
        if self.count {
            let count = db.count().await?;
            println!("count = {}", count);
        }

        Ok(())
    }
}

fn rand_value(size: usize, ratio: f64) -> Vec<u8> {
    let part_size = (size as f64 * ratio) as usize;
    let part: Vec<u8> = thread_rng()
        .sample_iter(Alphanumeric)
        .take(part_size)
        .collect();
    part.repeat((1f64 / ratio) as usize)
}

async fn bench_put(db: Arc<Database>, config: Config) {
    let mut tasks = Vec::new();
    let barrier = Arc::new(Barrier::new(config.num_put_tasks));
    let num_entries_per_task = config.num_entries / config.num_put_tasks;

    let now = Instant::now();
    for task_id in 0..config.num_put_tasks {
        let db = db.clone();
        let config = config.clone();
        let barrier = barrier.clone();
        let value = vec![0; config.value_size];
        let start = task_id * num_entries_per_task;
        let end = (task_id + 1) * num_entries_per_task;
        let task = tokio::task::spawn(async move {
            barrier.wait().await;
            for i in start..end {
                let key = i.to_be_bytes().to_vec();
                if let Some(ratio) = config.compression_ratio {
                    let value = rand_value(config.value_size, ratio);
                    db.timed_put(key, value).await.unwrap();
                } else {
                    db.timed_put(key, value.clone()).await.unwrap();
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

async fn bench_get(db: Arc<Database>, mut config: Config, warmup: bool) {
    let mut tasks = Vec::new();
    let barrier = Arc::new(Barrier::new(config.num_get_tasks));
    if warmup {
        config.num_entries /= 10;
    }
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
                for i in start..end {
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
