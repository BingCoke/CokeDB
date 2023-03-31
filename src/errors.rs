use std::{fmt::{Display, self}, str::FromStr};


/// 包装自己的返回result
pub type Result<T> = std::result::Result<T, Error>;


#[derive(Debug, Clone)]
pub enum Error {
    Parse(String),
    Schema(String),
    Plan(String),
    Evaluate(String)
}  

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> fmt::Result {
        match self {
            Error::Parse(s) | Error::Schema(s) | Error::Plan(s) | Error::Evaluate(s)  => {
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
impl From<std::num::ParseIntError> for Error {
    fn from(value: std::num::ParseIntError) -> Self {
        Error::Parse(value.to_string())
    }
}
