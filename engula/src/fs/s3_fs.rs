use std::sync::Arc;

use async_trait::async_trait;
use aws_sdk_s3::{
    client::Client,
    config::Config,
    model::{
        CompletedMultipartUpload, CompletedPart, ExpressionType, InputSerialization, JsonOutput,
        OutputSerialization, ParquetInput, SelectObjectContentEventStream,
    },
    Region,
};
use aws_types::credentials::Credentials;
use bytes::Buf;
use futures::executor::block_on;
use metrics::{counter, histogram};
use tokio::{task, time::Instant};
use tracing::error;

use crate::{
    error::{Error, Result},
    fs::{Fs, RandomAccessReader, SequentialWriter},
};

#[derive(Debug)]
pub struct S3Options {
    pub region: String,
    pub bucket: String,
    pub access_key: String,
    pub secret_access_key: String,
}

pub struct S3Fs {
    client: Client,
    bucket: String,
}

impl S3Fs {
    pub fn new(options: S3Options) -> S3Fs {
        let region = Region::new(options.region);
        let credentials =
            Credentials::from_keys(options.access_key, options.secret_access_key, None);
        let config = Config::builder()
            .region(Some(region))
            .credentials_provider(credentials)
            .build();
        let client = Client::from_conf(config);
        S3Fs {
            client,
            bucket: options.bucket,
        }
    }

    #[allow(dead_code)]
    async fn list(&self) -> Result<Vec<String>> {
        let output = self
            .client
            .list_objects()
            .bucket(self.bucket.clone())
            .send()
            .await?;
        Ok(output
            .contents
            .unwrap()
            .into_iter()
            .map(|x| x.key.unwrap())
            .collect())
    }

    async fn select(&self, key: &str, expr: &str) -> Result<Vec<u8>> {
        let parquet_input = InputSerialization::builder()
            .parquet(ParquetInput::builder().build())
            .build();
        let json_output = OutputSerialization::builder()
            .json(JsonOutput::builder().build())
            .build();
        let mut output = self
            .client
            .select_object_content()
            .bucket(self.bucket.clone())
            .key(key)
            .expression(expr)
            .expression_type(ExpressionType::Sql)
            .input_serialization(parquet_input)
            .output_serialization(json_output)
            .send()
            .await?;
        // Can't get it compiled in async mode, maybe figure it out later.
        match block_on(output.payload.recv()) {
            Ok(event) => {
                if let Some(SelectObjectContentEventStream::Records(record)) = event {
                    Ok(record.payload.unwrap().into_inner())
                } else {
                    error!("invalid select event: {:?}", event);
                    Err(Error::AwsSdk("invalid select event".to_owned()))
                }
            }
            Err(err) => {
                error!("select object {}: {}", key, err);
                Err(Error::AwsSdk(err.to_string()))
            }
        }
    }

    fn new_object(&self, key: &str) -> S3Object {
        S3Object::new(self.client.clone(), self.bucket.clone(), key.to_owned())
    }
}

#[async_trait]
impl Fs for S3Fs {
    async fn new_sequential_writer(&self, fname: &str) -> Result<Box<dyn SequentialWriter>> {
        let object = self.new_object(fname);
        let writer = S3Writer::new(object).await?;
        Ok(Box::new(writer))
    }

    async fn new_random_access_reader(&self, fname: &str) -> Result<Box<dyn RandomAccessReader>> {
        let object = self.new_object(fname);
        Ok(Box::new(object))
    }

    async fn count_file(&self, fname: &str) -> Result<usize> {
        let data = self.select(fname, "SELECT count(*) from S3Object").await?;
        let text = String::from_utf8(data).unwrap();
        let count = text.parse().unwrap();
        Ok(count)
    }

    async fn remove_file(&self, fname: &str) -> Result<()> {
        let result = self
            .client
            .delete_object()
            .bucket(self.bucket.clone())
            .key(fname.to_owned())
            .send()
            .await;
        match result {
            Ok(_) => Ok(()),
            Err(err) => {
                error!("[{}] remove file {}: {}", self.bucket, fname, err);
                Err(err.into())
            }
        }
    }
}

struct S3Object {
    client: Client,
    bucket: String,
    key: String,
    tag: String,
}

impl S3Object {
    fn new(client: Client, bucket: String, key: String) -> S3Object {
        let tag = format!("{}:{}", bucket, key);
        S3Object {
            client,
            bucket,
            key,
            tag,
        }
    }
}

#[async_trait]
impl RandomAccessReader for S3Object {
    async fn read_at(&self, offset: u64, size: u64) -> Result<Vec<u8>> {
        let range = format!("bytes={}-{}", offset, offset + size);
        let start = Instant::now();
        let result = self
            .client
            .get_object()
            .bucket(self.bucket.clone())
            .key(self.key.clone())
            .range(range)
            .send()
            .await;
        match result {
            Ok(output) => match output.body.collect().await {
                Ok(mut bytes) => {
                    let throughput = size as f64 / start.elapsed().as_secs_f64();
                    counter!("engula.fs.s3.read.bytes", size as u64);
                    histogram!("engula.fs.s3.read.throughput", throughput);
                    let mut data = vec![0; bytes.remaining()];
                    bytes.copy_to_slice(&mut data);
                    Ok(data)
                }
                Err(err) => Err(Error::AwsSdk(err.to_string())),
            },
            Err(err) => {
                error!(
                    "[{}] read offset {} size {}: {}",
                    self.tag, offset, size, err
                );
                Err(err.into())
            }
        }
    }
}

struct S3Writer {
    object: Arc<S3Object>,
    upload_id: String,
    part_handles: Vec<task::JoinHandle<Result<CompletedPart>>>,
}

impl S3Writer {
    async fn new(object: S3Object) -> Result<S3Writer> {
        let result = object
            .client
            .create_multipart_upload()
            .bucket(object.bucket.clone())
            .key(object.key.clone())
            .send()
            .await;
        match result {
            Ok(output) => {
                let upload_id = output.upload_id.unwrap();
                Ok(S3Writer {
                    object: Arc::new(object),
                    upload_id,
                    part_handles: Vec::new(),
                })
            }
            Err(err) => {
                error!("[{}] create multipart upload: {}", object.tag, err);
                Err(err.into())
            }
        }
    }

    fn upload_part(&mut self, part: Vec<u8>) {
        let object = self.object.clone();
        let upload_id = self.upload_id.clone();
        let part_number = (self.part_handles.len() + 1) as i32;
        let part_handle = task::spawn(async move {
            let size = part.len();
            let start = Instant::now();
            let result = object
                .client
                .upload_part()
                .bucket(object.bucket.clone())
                .key(object.key.clone())
                .upload_id(upload_id.clone())
                .part_number(part_number)
                .body(part.into())
                .send()
                .await;
            let throughput = size as f64 / start.elapsed().as_secs_f64();
            counter!("engula.fs.s3.write.bytes", size as u64);
            histogram!("engula.fs.s3.write.throughput", throughput);
            match result {
                Ok(output) => {
                    let part = CompletedPart::builder()
                        .e_tag(output.e_tag.unwrap())
                        .part_number(part_number)
                        .build();
                    Ok(part)
                }
                Err(err) => {
                    error!(
                        "[{}] upload id {} part {}: {}",
                        object.tag, upload_id, part_number, err
                    );
                    Err(err.into())
                }
            }
        });
        self.part_handles.push(part_handle);
    }

    async fn collect_parts(&mut self) -> Result<Vec<CompletedPart>> {
        let mut parts = Vec::new();
        for handle in self.part_handles.split_off(0) {
            let part = handle.await??;
            parts.push(part);
        }
        Ok(parts)
    }
}

// S3 doesn't allow us to upload a part smaller than 5MB.
const UPLOAD_PART_SIZE: usize = 8 * 1024 * 1024;

#[async_trait]
impl SequentialWriter for S3Writer {
    async fn write(&mut self, data: Vec<u8>) {
        // Shortcut
        if data.len() < UPLOAD_PART_SIZE * 2 {
            self.upload_part(data);
            return;
        }

        let mut offset = 0;
        while offset < data.len() {
            let end = if data.len() - offset < UPLOAD_PART_SIZE * 2 {
                data.len()
            } else {
                offset + UPLOAD_PART_SIZE
            };
            self.upload_part(data[offset..end].to_vec());
            offset = end;
        }
    }

    async fn finish(&mut self) -> Result<()> {
        let parts = self.collect_parts().await?;
        let upload = CompletedMultipartUpload::builder()
            .set_parts(Some(parts))
            .build();
        let start = Instant::now();
        let result = self
            .object
            .client
            .complete_multipart_upload()
            .bucket(self.object.bucket.clone())
            .key(self.object.key.clone())
            .upload_id(self.upload_id.clone())
            .multipart_upload(upload)
            .send()
            .await;
        histogram!("engula.fs.s3.finish.seconds", start.elapsed());
        match result {
            Ok(_) => Ok(()),
            Err(err) => {
                error!(
                    "[{}] complete multipart upload id {}: {}",
                    self.object.tag, self.upload_id, err
                );
                Err(err.into())
            }
        }
    }

    fn suggest_buffer_size(&self) -> usize {
        UPLOAD_PART_SIZE
    }
}
