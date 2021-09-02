use std::fmt::Debug;

use thiserror::Error;
use tokio::sync::{mpsc, oneshot, watch};

#[derive(Error, Debug, Clone)]
pub enum Error {
    #[error("io error: `{0}`")]
    Io(String),
    #[error("task error: `{0}`")]
    Task(String),
    #[error("timeout error: `{0}`")]
    Timeout(String),
    #[error("channel error: `{0}`")]
    Channel(String),
    #[error("transport error: `{0}`")]
    Transport(String),
}

impl From<std::io::Error> for Error {
    fn from(error: std::io::Error) -> Error {
        Error::Io(error.to_string())
    }
}

impl From<tokio::task::JoinError> for Error {
    fn from(error: tokio::task::JoinError) -> Error {
        Error::Task(error.to_string())
    }
}

impl<T> From<mpsc::error::SendError<T>> for Error {
    fn from(error: mpsc::error::SendError<T>) -> Error {
        Error::Channel(error.to_string())
    }
}

impl<T> From<watch::error::SendError<T>> for Error {
    fn from(_: watch::error::SendError<T>) -> Error {
        Error::Channel("watch send error".to_owned())
    }
}

impl From<oneshot::error::RecvError> for Error {
    fn from(error: oneshot::error::RecvError) -> Error {
        Error::Channel(error.to_string())
    }
}

impl From<tokio::time::error::Elapsed> for Error {
    fn from(error: tokio::time::error::Elapsed) -> Error {
        Error::Timeout(error.to_string())
    }
}

impl From<tonic::transport::Error> for Error {
    fn from(error: tonic::transport::Error) -> Error {
        Error::Transport(error.to_string())
    }
}

impl From<Error> for tonic::Status {
    fn from(error: Error) -> tonic::Status {
        tonic::Status::new(tonic::Code::Internal, error.to_string())
    }
}

pub type Result<T> = std::result::Result<T, Error>;
