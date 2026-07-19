use std::{fmt, io};

use arrow::error::ArrowError;
use fsspec_data::InterchangeError;
use fsspec_rs::FsError;
use parquet::errors::ParquetError;

#[derive(Debug)]
pub enum DbError {
    NotFound(String),
    PermissionDenied(String),
    AlreadyExists(String),
    NotADirectory(String),
    IsADirectory(String),
    InvalidArgument(String),
    NotSupported(String),
    Io(io::Error),
    Arrow(ArrowError),
    Parquet(ParquetError),
    Other(String),
}

pub type Result<T> = std::result::Result<T, DbError>;

impl fmt::Display for DbError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DbError::NotFound(msg) => write!(f, "not found: {msg}"),
            DbError::PermissionDenied(msg) => write!(f, "permission denied: {msg}"),
            DbError::AlreadyExists(msg) => write!(f, "already exists: {msg}"),
            DbError::NotADirectory(msg) => write!(f, "not a directory: {msg}"),
            DbError::IsADirectory(msg) => write!(f, "is a directory: {msg}"),
            DbError::InvalidArgument(msg) => write!(f, "invalid argument: {msg}"),
            DbError::NotSupported(msg) => write!(f, "not supported: {msg}"),
            DbError::Io(err) => write!(f, "I/O error: {err}"),
            DbError::Arrow(err) => write!(f, "Arrow error: {err}"),
            DbError::Parquet(err) => write!(f, "Parquet error: {err}"),
            DbError::Other(msg) => write!(f, "{msg}"),
        }
    }
}

impl std::error::Error for DbError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            DbError::Io(err) => Some(err),
            DbError::Arrow(err) => Some(err),
            DbError::Parquet(err) => Some(err),
            _ => None,
        }
    }
}

impl From<DbError> for FsError {
    fn from(err: DbError) -> Self {
        match err {
            DbError::NotFound(msg) => FsError::NotFound(msg),
            DbError::PermissionDenied(msg) => FsError::PermissionDenied(msg),
            DbError::AlreadyExists(msg) => FsError::AlreadyExists(msg),
            DbError::NotADirectory(msg) => FsError::NotADirectory(msg),
            DbError::IsADirectory(msg) => FsError::IsADirectory(msg),
            DbError::InvalidArgument(msg) => FsError::InvalidArgument(msg),
            DbError::NotSupported(msg) => FsError::NotSupported(msg),
            DbError::Io(err) => FsError::IoError(err),
            DbError::Arrow(err) => FsError::Other(err.to_string()),
            DbError::Parquet(err) => FsError::Other(err.to_string()),
            DbError::Other(msg) => FsError::Other(msg),
        }
    }
}

impl From<io::Error> for DbError {
    fn from(err: io::Error) -> Self {
        match err.kind() {
            io::ErrorKind::NotFound => DbError::NotFound(err.to_string()),
            io::ErrorKind::PermissionDenied => DbError::PermissionDenied(err.to_string()),
            io::ErrorKind::AlreadyExists => DbError::AlreadyExists(err.to_string()),
            _ => DbError::Io(err),
        }
    }
}

impl From<ArrowError> for DbError {
    fn from(err: ArrowError) -> Self {
        DbError::Arrow(err)
    }
}

impl From<ParquetError> for DbError {
    fn from(err: ParquetError) -> Self {
        DbError::Parquet(err)
    }
}

impl From<InterchangeError> for DbError {
    fn from(err: InterchangeError) -> Self {
        match err {
            InterchangeError::Arrow(err) => DbError::Arrow(err),
            InterchangeError::Parquet(err) => DbError::Parquet(err),
            err => DbError::Other(err.to_string()),
        }
    }
}

impl From<sqlx::Error> for DbError {
    fn from(err: sqlx::Error) -> Self {
        match err {
            sqlx::Error::RowNotFound => DbError::NotFound("row not found".to_string()),
            sqlx::Error::Io(err) => DbError::from(err),
            sqlx::Error::Database(err) => match err.kind() {
                sqlx::error::ErrorKind::UniqueViolation => {
                    DbError::AlreadyExists(err.message().to_string())
                }
                sqlx::error::ErrorKind::ForeignKeyViolation
                | sqlx::error::ErrorKind::NotNullViolation
                | sqlx::error::ErrorKind::CheckViolation
                | sqlx::error::ErrorKind::ExclusionViolation => {
                    DbError::InvalidArgument(err.message().to_string())
                }
                _ => {
                    let code = err.code().map(|code| code.into_owned()).unwrap_or_default();
                    if matches!(code.as_str(), "28000" | "28P01" | "1044" | "1045") {
                        DbError::PermissionDenied(err.message().to_string())
                    } else {
                        DbError::Other(err.message().to_string())
                    }
                }
            },
            err => DbError::Other(err.to_string()),
        }
    }
}
