use std::{borrow::Cow, path::Path, sync::Arc};

use engula::*;
use serde_derive::Deserialize;
use url::Url;

#[derive(Deserialize, Debug)]
pub struct Config {
    pub bench_get: bool,
    pub num_tasks: usize,
    pub value_size: usize,
    pub num_entries: usize,
    pub aws: Option<AwsConfig>,
    pub compute: ComputeConfig,
    pub journal: JournalConfig,
    pub journal_urls: Option<Vec<String>>,
    pub storage: StorageConfig,
    pub storage_urls: Option<Vec<String>>,
    pub manifest_url: Option<String>,
    pub compaction_url: Option<String>,
}

impl Config {
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Config> {
        let data = std::fs::read(path)?;
        let text = std::str::from_utf8(&data).unwrap();
        let config = toml::from_str(text).unwrap();
        Ok(config)
    }

    pub async fn new_db(&self) -> Result<Arc<Database>> {
        let options = self.compute.as_options();
        let journal = self.new_journal().await?;
        let storage = self.new_storage(true).await?;
        let manifest = self.new_manifest(storage.clone()).await?;
        let db = Database::new(options, journal, storage, manifest).await?;
        Ok(Arc::new(db))
    }

    pub async fn new_journal(&self) -> Result<Arc<dyn Journal>> {
        let options = self.journal.as_options();
        if let Some(urls) = &self.journal_urls {
            let journal = QuorumJournal::new(urls, options).await?;
            Ok(Arc::new(journal))
        } else {
            let journal = LocalJournal::new(&self.journal.path, options)?;
            Ok(Arc::new(journal))
        }
    }

    pub async fn new_storage(&self, online: bool) -> Result<Arc<dyn Storage>> {
        if let Some(urls) = &self.storage_urls {
            // hot and cool storages
            assert!(urls.len() >= 1 && urls.len() <= 2);
            let hot = self.new_inner_storage(&urls[0]).await?;
            if let Some(url) = urls.get(1) {
                let cool = self.new_inner_storage(url).await?;
                let hybrid = if online {
                    // Online services read from the hot storage.
                    HybridStorage::new(hot.clone(), vec![hot, cool])
                } else {
                    // Offline services read from the cool storage.
                    HybridStorage::new(cool.clone(), vec![hot, cool])
                };
                Ok(Arc::new(hybrid))
            } else {
                Ok(hot)
            }
        } else {
            let url = Url::from_file_path(&self.storage.path).unwrap();
            self.new_inner_storage(url.as_str()).await
        }
    }

    pub async fn new_manifest(&self, storage: Arc<dyn Storage>) -> Result<Arc<dyn Manifest>> {
        if let Some(url) = &self.manifest_url {
            let manifest = RemoteManifest::new(url).await?;
            Ok(Arc::new(manifest))
        } else {
            let options = self.compute.as_manifest_options();
            let runtime = self.new_compaction().await?;
            let manifest = LocalManifest::new(options, storage, runtime);
            Ok(Arc::new(manifest))
        }
    }

    pub async fn new_compaction(&self) -> Result<Arc<dyn CompactionRuntime>> {
        if let Some(url) = &self.compaction_url {
            let compaction = RemoteCompaction::new(url).await?;
            Ok(Arc::new(compaction))
        } else {
            let storage = self.new_storage(false).await?;
            Ok(Arc::new(LocalCompaction::new(storage)))
        }
    }

    async fn new_fs(&self, url: &str) -> Result<Arc<dyn Fs>> {
        let parsed_url = Url::parse(url)?;
        match parsed_url.scheme() {
            "s3" => {
                let aws = self.aws.as_ref().unwrap();
                let options = S3Options {
                    region: aws.region.clone(),
                    bucket: parsed_url.path().to_owned(),
                    access_key: aws.access_key.clone(),
                    secret_access_key: aws.secret_access_key.clone(),
                };
                let fs = S3Fs::new(options);
                Ok(Arc::new(fs))
            }
            "file" => {
                let fs = LocalFs::new(parsed_url.path())?;
                Ok(Arc::new(fs))
            }
            "http" => {
                let fs = RemoteFs::new(url).await?;
                Ok(Arc::new(fs))
            }
            _ => panic!("invalid fs scheme: {}", parsed_url.scheme()),
        }
    }

    async fn new_inner_storage(&self, url: &str) -> Result<Arc<dyn Storage>> {
        let fs = self.new_fs(url).await?;
        let parsed_url = Url::parse(url)?;
        let format = match parsed_url.query_pairs().next() {
            Some((Cow::Borrowed("format"), Cow::Borrowed(format))) => format,
            _ => "sstable",
        };
        match format {
            "sstable" => {
                let options = self.storage.as_sstable_options();
                let storage = SstableStorage::new(fs, options);
                Ok(Arc::new(storage))
            }
            "parquet" => {
                let options = self.storage.as_parquet_options();
                let storage = ParquetStorage::new(fs, options);
                Ok(Arc::new(storage))
            }
            _ => panic!("invalid storage format: {}", format),
        }
    }
}

#[derive(Deserialize, Debug)]
pub struct AwsConfig {
    region: String,
    access_key: String,
    secret_access_key: String,
}

#[derive(Deserialize, Debug)]
pub struct ComputeConfig {
    num_shards: usize,
    num_levels: usize,
    memtable_size_mb: usize,
    write_channel_size: usize,
}

impl ComputeConfig {
    pub fn as_options(&self) -> Options {
        Options {
            num_shards: self.num_shards,
            memtable_size: self.memtable_size_mb * 1024 * 1024,
            write_channel_size: self.write_channel_size,
        }
    }

    pub fn as_manifest_options(&self) -> ManifestOptions {
        ManifestOptions {
            num_shards: self.num_shards,
            num_levels: self.num_levels,
        }
    }
}

#[derive(Deserialize, Debug)]
pub struct JournalConfig {
    pub path: String,
    pub sync: bool,
    pub chunk_size: usize,
}

impl JournalConfig {
    pub fn as_options(&self) -> JournalOptions {
        JournalOptions {
            sync: self.sync,
            chunk_size: self.chunk_size,
        }
    }
}

#[derive(Deserialize, Debug)]
pub struct StorageConfig {
    pub path: String,
    pub sst_block_size_kb: usize,
    pub row_group_size_mb: usize,
}

impl StorageConfig {
    pub fn as_sstable_options(&self) -> SstableOptions {
        SstableOptions {
            block_size: self.sst_block_size_kb * 1024,
            block_cache: None,
        }
    }

    pub fn as_parquet_options(&self) -> ParquetOptions {
        ParquetOptions {
            row_group_size: self.row_group_size_mb * 1024 * 1024,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;

    #[tokio::test]
    async fn test() {
        let dir = env!("CARGO_MANIFEST_DIR");
        let file = Path::new(dir).join("bin/config.toml");
        let config = Config::from_file(file).unwrap();
        config.new_db().await.unwrap();
    }
}
