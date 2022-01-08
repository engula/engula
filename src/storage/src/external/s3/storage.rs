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
    cell::RefCell,
    future::Future,
    io,
    pin::Pin,
    task::{Context, Poll},
};

use aws_config::Config;
use aws_sdk_s3::{
    error::{
        CompleteMultipartUploadError, CreateMultipartUploadError, GetObjectError,
        ListObjectsV2Error, UploadPartError,
    },
    model::{
        BucketLifecycleConfiguration, BucketLocationConstraint, CompletedMultipartUpload,
        CompletedPart, CreateBucketConfiguration, ExpirationStatus, LifecycleExpiration,
        LifecycleRule, LifecycleRuleFilter, NoncurrentVersionExpiration, Object,
    },
    output::{
        CompleteMultipartUploadOutput, CreateMultipartUploadOutput, GetObjectOutput,
        ListObjectsV2Output, UploadPartOutput,
    },
    ByteStream, Client, DateTime, SdkError,
};
use aws_smithy_http::byte_stream::{self, AggregatedBytes};
use bytes::{Buf, Bytes};
use chrono::Duration;
use engula_futures::{
    io::{RandomRead, SequentialWrite},
    stream::batch::ResultStream,
};

use crate::{async_trait, Error, Result};

const OBJECT_CATEGORY: &str = "buckets";
const META_PREFIX: &str = "engula-meta";

#[derive(Clone)]
pub struct Storage {
    client: Client,
    s3_bucket: String,
    tenant_id: String,
    category: String,
}

impl Storage {
    pub async fn new(
        tenant_id: impl Into<String>,
        s3_bucket: impl Into<String>,
        conf: &Config,
    ) -> Result<Self> {
        let client = Client::new(conf);
        let tenant_id = tenant_id.into();
        let s3_bucket = s3_bucket.into();
        let category = OBJECT_CATEGORY.to_owned();
        Self::create_s3_bucket_if_not_exist(client.clone(), &s3_bucket).await?;
        Ok(Self {
            tenant_id,
            client,
            s3_bucket,
            category,
        })
    }

    async fn create_s3_bucket_if_not_exist(
        client: Client,
        s3_bucket: impl Into<String>,
    ) -> Result<()> {
        let bucket = s3_bucket.into();
        let exist = match client.head_bucket().bucket(&bucket).send().await {
            Ok(_) => Ok(true),
            Err(SdkError::ServiceError { err, .. }) if err.is_not_found() => Ok(false),
            Err(e) => Err(e),
        }?;
        if exist {
            return Ok(());
        }

        let bucket_config = CreateBucketConfiguration::builder()
            .set_location_constraint(Some(BucketLocationConstraint::UsWest2))
            .build();
        let result = client
            .create_bucket()
            .bucket(bucket)
            .create_bucket_configuration(bucket_config)
            .send()
            .await;
        match result {
            Ok(_) => Ok(()),
            Err(SdkError::ServiceError { err, .. }) if err.is_bucket_already_exists() => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    fn meta_root(&self) -> String {
        format!("{}/{}/{}", self.tenant_id, self.category, META_PREFIX)
    }

    // {tenant}/buckets/{meta_prefix}/{bucket_name}
    fn meta_key(&self, bucket: impl Into<String>) -> String {
        format!("{}/{}", self.meta_root(), bucket.into(),)
    }

    fn object_root(&self, bucket: impl Into<String>) -> String {
        format!("{}/{}/{}", self.tenant_id, self.category, bucket.into())
    }

    // {tenant}/buckets/{bucket_name}/{object_name}
    fn object_key(&self, engula_bucket: impl Into<String>, object: impl Into<String>) -> String {
        format!("{}/{}", self.object_root(engula_bucket), object.into())
    }
}

#[async_trait]
impl crate::Storage for Storage {
    type BucketLister = ListObjectStream;
    type ObjectLister = ListObjectStream;
    type RandomReader = RandomReader;
    type SequentialWriter = SequentialWriter;

    async fn list_buckets(&self) -> Result<Self::BucketLister> {
        Ok(ListObjectStream::new(
            self.client.clone(),
            self.s3_bucket.clone(),
            self.meta_root(),
            |o, prefix| match o.key.to_owned() {
                Some(k) => k.strip_prefix(prefix).map(|k| k.to_string()),
                None => None,
            },
        ))
    }

    async fn create_bucket(&self, bucket_name: &str) -> Result<()> {
        if bucket_name == META_PREFIX {
            return Err(Error::InvalidArgument(format!(
                "{} has be used by internal, please choose another bucket name",
                bucket_name.to_owned()
            )));
        }
        self.client
            .clone()
            .put_object()
            .bucket(&self.s3_bucket)
            .key(self.meta_key(bucket_name))
            .send()
            .await?;
        Ok(())
    }

    async fn delete_bucket(&self, bucket_name: &str) -> Result<()> {
        self.client
            .clone()
            .delete_object()
            .bucket(&self.s3_bucket)
            .key(self.meta_key(bucket_name))
            .send()
            .await?;

        let expire_ts = (chrono::Utc::today() - Duration::days(1))
            .and_hms(0, 0, 0)
            .timestamp();
        let rule = LifecycleRule::builder()
            .status(ExpirationStatus::Enabled)
            .filter(LifecycleRuleFilter::Prefix(self.object_root(bucket_name)))
            .expiration(
                LifecycleExpiration::builder()
                    .date(DateTime::from_secs(expire_ts))
                    .build(),
            )
            .noncurrent_version_expiration(
                NoncurrentVersionExpiration::builder()
                    .noncurrent_days(1)
                    .build(),
            )
            .build();
        let lifecyle = BucketLifecycleConfiguration::builder().rules(rule).build();
        self.client
            .clone()
            .put_bucket_lifecycle_configuration()
            .bucket(&self.s3_bucket)
            .lifecycle_configuration(lifecyle)
            .send()
            .await?;
        Ok(())
    }

    async fn list_objects(&self, bucket_name: &str) -> Result<Self::ObjectLister> {
        let prefix = self.object_root(bucket_name);
        Ok(ListObjectStream::new(
            self.client.clone(),
            self.s3_bucket.clone(),
            prefix,
            |o, prefix| match o.key.to_owned() {
                Some(k) => k.strip_prefix(prefix).map(|k| k.to_string()),
                None => None,
            },
        ))
    }

    async fn delete_object(&self, bucket_name: &str, object_name: &str) -> Result<()> {
        self.client
            .clone()
            .delete_object()
            .bucket(&self.s3_bucket)
            .key(self.object_key(bucket_name, object_name))
            .send()
            .await?;
        Ok(())
    }

    async fn new_random_reader(
        &self,
        bucket_name: &str,
        object_name: &str,
    ) -> Result<Self::RandomReader> {
        Ok(RandomReader::new(
            self.client.clone(),
            self.s3_bucket.clone(),
            self.object_key(bucket_name, object_name),
        ))
    }

    async fn new_sequential_writer(
        &self,
        bucket_name: &str,
        object_name: &str,
    ) -> Result<Self::SequentialWriter> {
        Ok(SequentialWriter::new(
            self.client.clone(),
            self.s3_bucket.clone(),
            self.object_key(bucket_name, object_name),
        ))
    }
}

type PinnedFuture<T, E> = Pin<Box<dyn Future<Output = std::result::Result<T, E>> + 'static + Send>>;

pub struct ListObjectStream {
    client: Client,
    bucket: String,
    prefix: String,

    object_extract: fn(&Object, &str) -> Option<String>,

    list_fut: Option<PinnedFuture<ListObjectsV2Output, SdkError<ListObjectsV2Error>>>,
    list_token: Option<String>,
    teriminated: bool,
}

impl ListObjectStream {
    pub fn new(
        client: Client,
        bucket: String,
        prefix: String,
        f: fn(&Object, &str) -> Option<String>,
    ) -> Self {
        Self {
            client,
            bucket,
            prefix,
            list_fut: None,
            list_token: None,
            object_extract: f,
            teriminated: false,
        }
    }
}

impl ResultStream for ListObjectStream {
    type Elem = String;
    type Error = Error;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        batch_size: usize,
    ) -> Poll<Result<Vec<Self::Elem>>> {
        let this = self.get_mut();

        if this.teriminated {
            return Poll::Ready(Ok(vec![]));
        }

        let mut fut = this.list_fut.take().unwrap_or_else(|| {
            let mut l = this
                .client
                .clone()
                .list_objects_v2()
                .bucket(&this.bucket)
                .prefix(&this.prefix)
                .max_keys(batch_size as i32);
            if let Some(token) = this.list_token.take() {
                l = l.continuation_token(token);
            }
            Box::pin(l.send())
        });

        match fut.as_mut().poll(cx) {
            Poll::Ready(Ok(output)) => {
                this.list_token = output.next_continuation_token.clone();
                this.teriminated = !output.is_truncated();
                let prefix = this.prefix.to_owned() + "/";
                match output.contents {
                    Some(contents) => {
                        let buckets = contents
                            .iter()
                            .filter_map(|c| (this.object_extract)(c, &prefix))
                            .collect::<Vec<String>>();
                        Poll::Ready(Ok(buckets))
                    }
                    None => Poll::Ready(Ok(vec![])),
                }
            }
            Poll::Ready(Err(e)) => Poll::Ready(Err(e.into())),
            Poll::Pending => {
                this.list_fut = Some(fut);
                Poll::Pending
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, None)
    }
}

pub struct RandomReader {
    client: Client,
    bucket: String,
    key: String,

    inner: RefCell<ReaderInner>,
}

pub struct ReaderInner {
    state: ReadState,
}

enum ReadState {
    Sending,
    WaitHead(PinnedFuture<GetObjectOutput, SdkError<GetObjectError>>),
    WaitBody(PinnedFuture<AggregatedBytes, byte_stream::Error>),
}

impl RandomReader {
    fn new(client: Client, bucket: impl Into<String>, key: impl Into<String>) -> Self {
        Self {
            client,
            bucket: bucket.into(),
            key: key.into(),
            inner: RefCell::new(ReaderInner {
                state: ReadState::Sending,
            }),
        }
    }
}

impl RandomRead for RandomReader {
    fn poll_read(
        self: Pin<&Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
        pos: usize,
    ) -> Poll<io::Result<usize>> {
        let this = self.get_ref();
        let mut inner = this.inner.borrow_mut();
        loop {
            match inner.state {
                ReadState::Sending => {
                    let range = format!("bytes={}-{}", pos, pos + buf.len() - 1);
                    let f = Box::pin(
                        this.client
                            .clone()
                            .get_object()
                            .bucket(&this.bucket)
                            .key(&this.key)
                            .range(range)
                            .send(),
                    );
                    inner.state = ReadState::WaitHead(f);
                }
                ReadState::WaitHead(ref mut fut) => match fut.as_mut().poll(cx) {
                    Poll::Ready(r) => match r {
                        Ok(output) => {
                            let body: ByteStream = output.body;
                            let f = Box::pin(body.collect());
                            inner.state = ReadState::WaitBody(f);
                        }
                        Err(e) => {
                            let e = io::Error::new(io::ErrorKind::Other, e);
                            return Poll::Ready(Err(e));
                        }
                    },
                    Poll::Pending => {
                        return Poll::Pending;
                    }
                },
                ReadState::WaitBody(ref mut fut) => match fut.as_mut().poll(cx) {
                    Poll::Ready(r) => match r {
                        Ok(mut bytes) => {
                            let size = bytes.remaining();
                            bytes.copy_to_slice(buf);
                            inner.state = ReadState::Sending;
                            return Poll::Ready(Ok(size));
                        }
                        Err(e) => {
                            let e = io::Error::new(io::ErrorKind::Other, e);
                            return Poll::Ready(Err(e));
                        }
                    },
                    Poll::Pending => {
                        return Poll::Pending;
                    }
                },
            }
        }
    }
}

pub struct SequentialWriter {
    client: Client,
    bucket: String,
    key: String,

    write_buf: Vec<u8>,
    state: WriterState,
}

enum WriterState {
    Creating,
    WaitCreate(PinnedFuture<CreateMultipartUploadOutput, SdkError<CreateMultipartUploadError>>),
    UploadingParts(String),
    WaitPartUpload {
        parts: Vec<UploadPartState>,
        upload_id: String,
    },
    Completing {
        parts: Vec<CompletedPart>,
        upload_id: String,
    },
    WaitComplete(
        PinnedFuture<CompleteMultipartUploadOutput, SdkError<CompleteMultipartUploadError>>,
    ),
}

enum UploadPartState {
    Pending(PinnedFuture<UploadPartOutput, SdkError<UploadPartError>>),
    Done(CompletedPart),
}

impl SequentialWriter {
    fn new(client: Client, bucket: impl Into<String>, key: impl Into<String>) -> Self {
        Self {
            client,
            bucket: bucket.into(),
            key: key.into(),
            state: WriterState::Creating,
            write_buf: vec![],
        }
    }

    fn start_upload(
        client: Client,
        bucket: impl Into<String>,
        key: impl Into<String>,
    ) -> PinnedFuture<CreateMultipartUploadOutput, SdkError<CreateMultipartUploadError>> {
        Box::pin(
            client
                .create_multipart_upload()
                .bucket(bucket)
                .key(key)
                .send(),
        )
    }

    fn upload_part(
        client: Client,
        bucket: impl Into<String>,
        key: impl Into<String>,
        upload_id: impl Into<String>,
        part_num: i32,
        data: Bytes,
    ) -> PinnedFuture<UploadPartOutput, SdkError<UploadPartError>> {
        Box::pin(
            client
                .upload_part()
                .bucket(bucket)
                .key(key)
                .upload_id(upload_id)
                .part_number(part_num)
                .body(ByteStream::from(data))
                .send(),
        )
    }

    fn finish_upload(
        client: Client,
        bucket: impl Into<String>,
        key: impl Into<String>,
        upload_id: impl Into<String>,
        parts: Vec<CompletedPart>,
    ) -> PinnedFuture<CompleteMultipartUploadOutput, SdkError<CompleteMultipartUploadError>> {
        let upload = CompletedMultipartUpload::builder()
            .set_parts(Some(parts))
            .build();
        Box::pin(
            client
                .complete_multipart_upload()
                .bucket(bucket)
                .key(key)
                .upload_id(upload_id)
                .multipart_upload(upload)
                .send(),
        )
    }
}

const UPLOAD_PART_SIZE: usize = 8 * 1024 * 1024;

impl SequentialWrite for SequentialWriter {
    fn poll_write(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        this.write_buf.extend(buf);
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        loop {
            match this.state {
                WriterState::Creating => {
                    this.state = WriterState::WaitCreate(Self::start_upload(
                        this.client.clone(),
                        this.bucket.to_owned(),
                        this.key.to_owned(),
                    ))
                }
                WriterState::WaitCreate(ref mut fut) => match fut.as_mut().poll(cx) {
                    Poll::Ready(Ok(ref mut output)) => {
                        this.state = WriterState::UploadingParts(output.upload_id.take().unwrap());
                    }
                    Poll::Ready(Err(err)) => {
                        let e = io::Error::new(io::ErrorKind::Other, err);
                        return Poll::Ready(Err(e));
                    }
                    Poll::Pending => {
                        return Poll::Pending;
                    }
                },
                WriterState::UploadingParts(ref upload_id) => {
                    let mut futs: Vec<UploadPartState> = Vec::new();

                    if this.write_buf.len() < UPLOAD_PART_SIZE * 2 {
                        let part_num = (futs.len() + 1) as i32;
                        futs.push(UploadPartState::Pending(Self::upload_part(
                            this.client.clone(),
                            this.bucket.to_owned(),
                            this.key.to_owned(),
                            upload_id.to_owned(),
                            part_num,
                            Bytes::from(this.write_buf.clone()),
                        )));
                        this.state = WriterState::WaitPartUpload {
                            parts: futs,
                            upload_id: upload_id.to_owned(),
                        };
                        continue;
                    }

                    while this.write_buf.len() >= UPLOAD_PART_SIZE * 2 {
                        let part_num = (futs.len() + 1) as i32;
                        let remain = this.write_buf.split_off(UPLOAD_PART_SIZE);
                        futs.push(UploadPartState::Pending(Self::upload_part(
                            this.client.clone(),
                            this.bucket.to_owned(),
                            this.key.to_owned(),
                            upload_id.to_owned(),
                            part_num,
                            Bytes::from(this.write_buf.clone()),
                        )));
                        this.write_buf = remain;
                    }
                    let part_num = (futs.len() + 1) as i32;
                    futs.push(UploadPartState::Pending(Self::upload_part(
                        this.client.clone(),
                        this.bucket.to_owned(),
                        this.key.to_owned(),
                        upload_id.to_owned(),
                        part_num,
                        Bytes::from(this.write_buf.clone()),
                    )));
                    this.state = WriterState::WaitPartUpload {
                        parts: futs,
                        upload_id: upload_id.to_owned(),
                    };
                }
                WriterState::WaitPartUpload {
                    ref mut parts,
                    ref upload_id,
                } => {
                    let mut all_done = true;
                    for idx in 0..parts.len() {
                        let part_num = idx as i32 + 1;
                        let part_done = match parts[idx] {
                            UploadPartState::Pending(ref mut fut) => match fut.as_mut().poll(cx) {
                                Poll::Ready(Ok(output)) => Ok(output),
                                Poll::Ready(Err(err)) => Err(err),
                                Poll::Pending => {
                                    all_done = false;
                                    continue;
                                }
                            },
                            UploadPartState::Done(ref mut _r) => continue,
                        };

                        match part_done {
                            Ok(output) => {
                                let completed = CompletedPart::builder()
                                    .e_tag(output.e_tag.unwrap())
                                    .part_number(part_num)
                                    .build();
                                parts[(part_num - 1) as usize] = UploadPartState::Done(completed)
                            }
                            Err(err) => {
                                let e = io::Error::new(io::ErrorKind::Other, err);
                                return Poll::Ready(Err(e));
                            }
                        }
                    }

                    if all_done {
                        let elems = std::mem::take(parts);
                        let completed_parts: Vec<CompletedPart> = elems
                            .into_iter()
                            .map(|e| match e {
                                UploadPartState::Done(t) => t,
                                _ => unreachable!(),
                            })
                            .collect();
                        this.state = WriterState::Completing {
                            parts: completed_parts,
                            upload_id: upload_id.to_owned(),
                        };
                    } else {
                        return Poll::Pending;
                    }
                }
                WriterState::Completing {
                    ref parts,
                    ref upload_id,
                } => {
                    this.state = WriterState::WaitComplete(Self::finish_upload(
                        this.client.clone(),
                        this.bucket.clone(),
                        this.key.clone(),
                        upload_id.clone(),
                        parts.clone(),
                    ));
                }
                WriterState::WaitComplete(ref mut fut) => match fut.as_mut().poll(cx) {
                    Poll::Ready(Ok(_)) => return Poll::Ready(Ok(())),
                    Poll::Ready(Err(err)) => {
                        let e = io::Error::new(io::ErrorKind::Other, err);
                        return Poll::Ready(Err(e));
                    }
                    Poll::Pending => {
                        return Poll::Pending;
                    }
                },
            }
        }
    }
}
