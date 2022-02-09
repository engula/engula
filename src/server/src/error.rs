use thiserror::Error;
use tonic::{Code, Status};

#[derive(Error, Debug)]
pub enum Error {
    #[error("{0} is not found")]
    NotFound(String),
    #[error("{0} already exists")]
    AlreadyExists(String),
    #[error("invalid request")]
    InvalidRequest,
}

impl From<Error> for Status {
    fn from(err: Error) -> Status {
        let (code, message) = match err {
            Error::NotFound(m) => (Code::NotFound, m),
            Error::AlreadyExists(m) => (Code::AlreadyExists, m),
            Error::InvalidRequest => (Code::InvalidArgument, "invalid request".to_owned()),
        };
        Status::new(code, message)
    }
}

pub type Result<T> = std::result::Result<T, Error>;
