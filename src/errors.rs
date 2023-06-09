use std::{
    array::TryFromSliceError,
    fmt::{self, Display},
    str::FromStr,
    string::FromUtf8Error,
    sync::PoisonError,
};

use log::{ParseLevelError, SetLoggerError};
use serde_derive::{Deserialize, Serialize};

/// 包装自己的返回result
pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Error {
    Parse(String),
    Schema(String),
    Plan(String),
    Evaluate(String),
    Optimizer(String),
    Encoding(String),
    BinCode(String),
    Table(String),
    Row(String),
    Internal(String),
    Lock(String),
    Mvcc(String),
    Index(String),
    Executor(String),
    IO(String),
    Rustyline(String),
    Config(String),
    LogError(String)
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> fmt::Result {
        use Error::*;
        match self {
            LogError(s)|Config(s) | Rustyline(s) | IO(s) | Executor(s) | Index(s) | Mvcc(s) | Lock(s)
            | Internal(s) | Row(s) | Table(s) | BinCode(s) | Parse(s) | Schema(s) | Plan(s)
            | Evaluate(s) | Optimizer(s) | Encoding(s) => {
                write!(f, "{}", s)
            }
        }
    }
}

impl From<regex::Error> for Error {
    fn from(value: regex::Error) -> Self {
        Error::Evaluate(value.to_string())
    }
}

impl From<TryFromSliceError> for Error {
    fn from(value: TryFromSliceError) -> Self {
        Error::Encoding(value.to_string())
    }
}

impl From<FromUtf8Error> for Error {
    fn from(value: FromUtf8Error) -> Self {
        Error::Encoding(value.to_string())
    }
}

impl From<std::num::ParseIntError> for Error {
    fn from(value: std::num::ParseIntError) -> Self {
        Error::Parse(value.to_string())
    }
}

impl From<Box<bincode::ErrorKind>> for Error {
    fn from(value: Box<bincode::ErrorKind>) -> Self {
        Self::Encoding(value.to_string())
    }
}

impl<T> From<PoisonError<T>> for Error {
    fn from(value: PoisonError<T>) -> Self {
        Self::Lock(format!("error get value {:?} lock", value))
    }
}

impl From<std::io::Error> for Error {
    fn from(value: std::io::Error) -> Self {
        let io_err = value.to_string();
        Self::IO(io_err)
    }
}

impl From<tokio::task::JoinError> for Error {
    fn from(err: tokio::task::JoinError) -> Self {
        Error::Internal(err.to_string())
    }
}

impl<T> From<tokio::sync::mpsc::error::SendError<T>> for Error {
    fn from(err: tokio::sync::mpsc::error::SendError<T>) -> Self {
        Error::Internal(err.to_string())
    }
}

impl<T> From<tokio::sync::mpsc::error::TrySendError<T>> for Error {
    fn from(err: tokio::sync::mpsc::error::TrySendError<T>) -> Self {
        Error::Internal(err.to_string())
    }
}

impl From<tokio::sync::oneshot::error::RecvError> for Error {
    fn from(err: tokio::sync::oneshot::error::RecvError) -> Self {
        Error::Internal(err.to_string())
    }
}
impl From<rustyline::error::ReadlineError> for Error {
    fn from(value: rustyline::error::ReadlineError) -> Self {
        Error::Rustyline(value.to_string())
    }
}

impl From<config::ConfigError> for Error {
    fn from(value: config::ConfigError) -> Self {
        Error::Config(value.to_string())
    }
}



impl From<ParseLevelError> for Error {
    fn from(value: ParseLevelError) -> Self {
        Error::Config(value.to_string())
    }
}


impl From<SetLoggerError> for Error {
    fn from(value: SetLoggerError) -> Self {
        Error::LogError(value.to_string())
    }
}
