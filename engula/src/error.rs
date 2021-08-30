use std::fmt::Debug;

use thiserror::Error;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task;

#[derive(Error, Debug, Clone)]
pub enum Error {
    #[error("io error: `{0}`")]
    IoError(String),
    #[error("task error: `{0}`")]
    TaskError(String),
    #[error("channel error: `{0}`")]
    ChannelError(String),
}

impl From<std::io::Error> for Error {
    fn from(error: std::io::Error) -> Error {
        Error::IoError(error.to_string())
    }
}

impl From<task::JoinError> for Error {
    fn from(error: task::JoinError) -> Error {
        Error::TaskError(error.to_string())
    }
}

impl<T> From<mpsc::error::SendError<T>> for Error {
    fn from(error: mpsc::error::SendError<T>) -> Error {
        Error::ChannelError(error.to_string())
    }
}

impl<T> From<watch::error::SendError<T>> for Error {
    fn from(_: watch::error::SendError<T>) -> Error {
        Error::ChannelError("watch send error".to_owned())
    }
}

impl From<oneshot::error::RecvError> for Error {
    fn from(error: oneshot::error::RecvError) -> Error {
        Error::ChannelError(error.to_string())
    }
}

pub type Result<T> = std::result::Result<T, Error>;
