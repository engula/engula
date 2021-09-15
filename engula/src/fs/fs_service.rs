use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use futures::StreamExt;
use tokio::sync::{Mutex, RwLock};
use tonic::{Code, Request, Response, Status, Streaming};

use super::{proto::*, Fs, RandomAccessReader, SequentialWriter};

type ReaderRef = Arc<dyn RandomAccessReader>;
type WriterRef = Arc<Mutex<Box<dyn SequentialWriter>>>;

pub struct FsService {
    fs: Box<dyn Fs>,
    next_fd: AtomicU64,
    readers: RwLock<HashMap<u64, ReaderRef>>,
    writers: RwLock<HashMap<u64, WriterRef>>,
    opened_files: Mutex<HashMap<String, u64>>,
}

impl FsService {
    pub fn new(fs: Box<dyn Fs>) -> FsService {
        FsService {
            fs,
            next_fd: AtomicU64::new(0),
            writers: RwLock::new(HashMap::new()),
            readers: RwLock::new(HashMap::new()),
            opened_files: Mutex::new(HashMap::new()),
        }
    }

    async fn get_reader(&self, fd: u64) -> Option<ReaderRef> {
        self.readers.read().await.get(&fd).cloned()
    }

    async fn get_writer(&self, fd: u64) -> Option<WriterRef> {
        self.writers.read().await.get(&fd).cloned()
    }
}

#[tonic::async_trait]
impl fs_server::Fs for FsService {
    async fn open(&self, request: Request<OpenRequest>) -> Result<Response<OpenResponse>, Status> {
        let input = request.into_inner();
        let fd = self.next_fd.fetch_add(1, Ordering::SeqCst);
        if input.access_mode == AccessMode::Read as i32 {
            let reader = self.fs.new_random_access_reader(&input.file_name).await?;
            let reader = Arc::from(reader);
            let mut readers = self.readers.write().await;
            readers.insert(fd, reader);
        } else {
            let writer = self.fs.new_sequential_writer(&input.file_name).await?;
            let writer = Arc::new(Mutex::new(writer));
            let mut writers = self.writers.write().await;
            writers.insert(fd, writer);
        }
        self.opened_files.lock().await.insert(input.file_name, fd);
        let output = OpenResponse { fd };
        Ok(Response::new(output))
    }

    async fn read(&self, request: Request<ReadRequest>) -> Result<Response<ReadResponse>, Status> {
        let input = request.into_inner();
        if let Some(reader) = self.get_reader(input.fd).await {
            let data = reader.read_at(input.offset, input.size).await?;
            Ok(Response::new(ReadResponse { data }))
        } else {
            Err(Status::new(
                Code::NotFound,
                format!("fd {} not found", input.fd),
            ))
        }
    }

    async fn write(
        &self,
        request: Request<Streaming<WriteRequest>>,
    ) -> Result<Response<WriteResponse>, Status> {
        let mut stream = request.into_inner();
        while let Some(input) = stream.next().await {
            let input = input?;
            if let Some(writer) = self.get_writer(input.fd).await {
                writer.lock().await.write(input.data).await;
            } else {
                return Err(Status::new(
                    Code::NotFound,
                    format!("fd {} not found", input.fd),
                ));
            }
        }
        Ok(Response::new(WriteResponse::default()))
    }

    async fn finish(
        &self,
        request: Request<FinishRequest>,
    ) -> Result<Response<FinishResponse>, Status> {
        let input = request.into_inner();
        if let Some(writer) = self.get_writer(input.fd).await {
            writer.lock().await.finish().await?;
            self.writers.write().await.remove(&input.fd);
            Ok(Response::new(FinishResponse::default()))
        } else {
            Err(Status::new(
                Code::NotFound,
                format!("fd {} not found", input.fd),
            ))
        }
    }

    async fn remove(
        &self,
        request: Request<RemoveRequest>,
    ) -> Result<Response<RemoveResponse>, Status> {
        let input = request.into_inner();
        let files = self.opened_files.lock().await;
        if let Some(fd) = files.get(&input.file_name) {
            self.readers.write().await.remove(fd);
            self.writers.write().await.remove(fd);
        }
        self.fs.remove_file(&input.file_name).await?;
        Ok(Response::new(RemoveResponse::default()))
    }
}
