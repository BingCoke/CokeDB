use std::fmt::{Display, self};



/// 包装自己的返回result
pub type Result<T> = std::result::Result<T, Error>;


#[derive(Debug, Clone)]
pub enum Error {
    Parse(String),
}  
impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> fmt::Result {
        match self {
            Error::Parse(s)  => {
                write!(f, "{}", s)
            }
        }
    }
}
