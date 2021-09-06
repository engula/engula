use crate::error::Result;

use aws_sdk_s3::{
    client::Client,
    config::Config,
    model::{ExpressionType, InputSerialization, JsonOutput, OutputSerialization, ParquetInput},
    output::SelectObjectContentOutput,
    Region,
};
use aws_types::credentials::Credentials;

#[allow(dead_code)]
pub struct S3Config {
    region: String,
    bucket: String,
    access_key: String,
    secret_access_key: String,
}

#[allow(dead_code)]
pub struct S3Bucket {
    client: Client,
    bucket: String,
}

#[allow(dead_code)]
impl S3Bucket {
    pub fn new(cfg: S3Config) -> Result<S3Bucket> {
        let region = Region::new(cfg.region);
        let credentials = Credentials::from_keys(cfg.access_key, cfg.secret_access_key, None);
        let config = Config::builder()
            .region(Some(region))
            .credentials_provider(credentials)
            .build();
        let client = Client::from_conf(config);
        Ok(S3Bucket {
            client,
            bucket: cfg.bucket,
        })
    }

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

    async fn select(&self, key: &str, expr: &str) -> Result<SelectObjectContentOutput> {
        let parquet_input = InputSerialization::builder()
            .parquet(ParquetInput::builder().build())
            .build();
        let json_output = OutputSerialization::builder()
            .json(JsonOutput::builder().build())
            .build();
        let output = self
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
        Ok(output)
    }
}
