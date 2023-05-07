pub mod mvcc;
pub mod encoding;
use std::{ops::{Bound, RangeBounds}, fmt::Display};
use crate::errors::*;

pub use mvcc::MVCC;


/// A key/value 存储
pub trait Store: Display + Send + Sync {
    /// 删除key
    fn delete(&mut self, key: &[u8]) -> Result<()>;

    /// flush数据
    fn flush(&mut self) -> Result<()>;

    /// get key
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;

    /// 规定一个范围进行kv查询
    fn scan(&self, range: MyRange) -> Scan;

    /// 设置key
    fn set(&mut self, key: &[u8], value: Vec<u8>) -> Result<()>;
}

/// 设置范围
pub struct MyRange {
    start: Bound<Vec<u8>>,
    end: Bound<Vec<u8>>,
}

impl MyRange {
    /// 设置自己的range
    pub fn new<R: RangeBounds<Vec<u8>>>(range: R) -> Self {
        Self {
            start: match range.start_bound() {
                Bound::Included(v) => Bound::Included(v.to_vec()),
                Bound::Excluded(v) => Bound::Excluded(v.to_vec()),
                Bound::Unbounded => Bound::Unbounded,
            },
            end: match range.end_bound() {
                Bound::Included(v) => Bound::Included(v.to_vec()),
                Bound::Excluded(v) => Bound::Excluded(v.to_vec()),
                Bound::Unbounded => Bound::Unbounded,
            },
        }
    }

    /// 检查传入的数据是否包含
    fn contains(&self, v: &[u8]) -> bool {
        (match &self.start {
            Bound::Included(start) => &**start <= v,
            Bound::Excluded(start) => &**start < v,
            Bound::Unbounded => true,
        }) && (match &self.end {
            Bound::Included(end) => v <= &**end,
            Bound::Excluded(end) => v < &**end,
            Bound::Unbounded => true,
        })
    }
}

impl RangeBounds<Vec<u8>> for MyRange {
    fn start_bound(&self) -> Bound<&Vec<u8>> {
        match &self.start {
            Bound::Included(v) => Bound::Included(v),
            Bound::Excluded(v) => Bound::Excluded(v),
            Bound::Unbounded => Bound::Unbounded,
        }
    }

    fn end_bound(&self) -> Bound<&Vec<u8>> {
        match &self.end {
            Bound::Included(v) => Bound::Included(v),
            Bound::Excluded(v) => Bound::Excluded(v),
            Bound::Unbounded => Bound::Unbounded,
        }
    }
}


pub type KvRange = Vec<Result<(Vec<u8>,Vec<u8>)>>;


pub type Scan = Box<dyn DoubleEndedIterator<Item = Result<(Vec<u8>, Vec<u8>)>> + Send>;
