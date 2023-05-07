use std::{
    array::TryFromSliceError,
    fmt::{self, Display},
    str::FromStr,
    string::FromUtf8Error,
    sync::PoisonError,
};

/// 包装自己的返回result
pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone)]
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
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> fmt::Result {
        use Error::*;
        match self {
            Index(s) | Mvcc(s) | Lock(s) | Internal(s) | Row(s) | Table(s) | BinCode(s)
            | Parse(s) | Schema(s) | Plan(s) | Evaluate(s) | Optimizer(s) | Encoding(s) => {
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
