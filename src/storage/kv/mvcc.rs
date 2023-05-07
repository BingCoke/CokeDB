use serde::Deserialize;
use serde::Serialize;
use serde_derive::Deserialize as DeserializeDerive;
use serde_derive::Serialize as SerializeDerive;

use crate::{
    errors::*,
    storage::kv::{encoding, MyRange},
};

use std::ops::Bound;
use std::{
    borrow::Cow,
    collections::HashSet,
    iter::Peekable,
    ops::RangeBounds,
    sync::{Arc, RwLock, RwLockReadGuard},
};

use super::Store;
use crate::errors::Result;

pub struct Status {
    pub txns: u64,
    pub txns_active: u64,
    pub storage: String,
}

#[derive(Clone)]
pub struct MVCC {
    store: Arc<RwLock<Box<dyn Store>>>,
}

impl MVCC {
    /// 创建一个mvcc
    pub fn new(store: Box<dyn Store>) -> Self {
        Self {
            store: Arc::new(RwLock::new(store)),
        }
    }

    /// 开启一个事务 基于给定的mode
    pub fn begin_with_mode(&self, mode: Mode) -> Result<MvccTransaction> {
        MvccTransaction::begin(self.store.clone(), mode)
    }

    /// 恢复事务
    pub fn resume(&self, id: u64) -> Result<MvccTransaction> {
        MvccTransaction::resume(self.store.clone(), id)
    }

    /// 设置 元数据
    pub fn set_metadata(&self, key: &[u8], value: Vec<u8>) -> Result<()> {
        let mut store = self.store.write()?;
        store.set(&Key::Metadata(key.into()).encode(), value)
    }

    /// 获得元数据
    pub fn get_metadata(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let store = self.store.read()?;
        store.get(&Key::Metadata(key.into()).encode())
    }

    /// 获得当前存储状态
    pub fn get_status(&self) -> Result<Status> {
        let store = self.store.read()?;
        return Ok(Status {
            txns: match store.get(&Key::TxnNext.encode())? {
                Some(ref v) => deserialize(v)?,
                None => 1,
            } - 1,
            txns_active: store
                .scan(MyRange::new(
                    Key::TxnActive(0).encode()..Key::TxnActive(std::u64::MAX).encode(),
                ))
                .into_iter()
                .try_fold(0, |count, r| r.map(|_| count + 1))?,
            storage: store.to_string(),
        });
    }
}

/// mvcc 事务模式
#[derive(Clone, Copy, Debug, PartialEq, SerializeDerive, DeserializeDerive)]
pub enum Mode {
    /// 可读可写事务
    ReadWrite,
    /// 只读事务
    ReadOnly,
    /// 只读事务 只读一个已经提交的事务
    Snapshot { version: u64 },
}

impl Mode {
    pub fn mutable(&self) -> bool {
        match self {
            Mode::ReadWrite => true,
            Mode::ReadOnly => false,
            Mode::Snapshot { .. } => false,
        }
    }
}

/// An MVCC transaction.
pub struct MvccTransaction {
    /// 存储
    store: Arc<RwLock<Box<dyn Store>>>,
    /// 唯一事务id
    id: u64,
    /// 事务模式
    mode: Mode,
    /// 快照 存储版本信息的
    snapshot: Snapshot,
}

impl MvccTransaction {
    /// 开启一个事务
    fn begin(store: Arc<RwLock<Box<dyn Store>>>, mode: Mode) -> Result<Self> {
        // 先找到新的
        let mut store_ = store.write()?;
        let next = store_.get(&Key::TxnNext.encode())?;
        let id: u64 = match next {
            Some(v) => deserialize(&v)?,
            None => 1,
        };
        // 设置出来下一个
        store_.set(&Key::TxnNext.encode(), serialize(&(id + 1))?)?;
        // 把当前活跃的事务设置一下
        store_.set(&Key::TxnActive(id).encode(), serialize(&mode)?)?;

        // 获取当前所有的活跃事务

        let scan = store_.scan(MyRange::new(
            Key::TxnActive(0).encode()..Key::TxnActive(id).encode(),
        ));
        let mut invisible = HashSet::new();
        for r in scan.into_iter() {
            let (k, _) = r?;
            match Key::decode(&k)? {
                Key::TxnActive(id) => invisible.insert(id),
                k => {
                    return Err(Error::Internal(format!(
                        "expect get TxnActive but get {:?}",
                        k
                    )))
                }
            };
        }

        // 设置保存一下快照
        store_.set(&Key::TxnSnapshot(id).encode(), serialize(&invisible)?)?;

        let snapshot = Snapshot::new(id, invisible);

        drop(store_);

        Ok(MvccTransaction {
            store,
            id,
            mode,
            snapshot,
        })
    }

    /// 恢复一个旧的活跃事务
    fn resume(store: Arc<RwLock<Box<dyn Store>>>, id: u64) -> Result<Self> {
        let store_ = store.read()?;

        let mode = match store_.get(&Key::TxnActive(id).encode())? {
            Some(v) => deserialize(&v)?,
            None => return Err(Error::Internal(format!("No active transaction {}", id))),
        };
        let snapshot = match &mode {
            Mode::Snapshot { version } => Snapshot::restore(&store_, *version)?,
            _ => Snapshot::restore(&store_, id)?,
        };
        std::mem::drop(store_);
        // 这个就是完全就是旧事务了
        Ok(Self {
            store,
            id,
            mode,
            snapshot,
        })
    }

    /// 获得当前事务的事务id
    pub fn get_id(&self) -> u64 {
        self.id
    }

    /// 获取当前事务的模式
    pub fn mode(&self) -> Mode {
        self.mode
    }

    /// 提交一个事务
    pub fn commit(&self) -> Result<()> {
        let mut store = self.store.write()?;
        // 将update key删除
        self.get_rollback_delete_update_key()?;

        store.delete(&Key::TxnActive(self.id).encode())?;

        store.flush()
    }

    /// 回滚当前事务
    pub fn rollback(&self) -> Result<()> {
        // 回滚的时候需要将当前version的key全部删除
        let rollback = self.get_rollback_delete_update_key()?;
        let mut store = self.store.write()?;
        for item in rollback {
            store.delete(&item)?;
        }
        store.delete(&Key::TxnActive(self.id).encode())?;

        store.flush()
    }

    fn get_rollback_delete_update_key(&self) -> Result<Vec<Vec<u8>>> {
        let mut roallback = Vec::new();
        let mut store = self.store.write()?;
        let scan = store.scan(MyRange::new(
            Key::TxnUpdate(self.id, vec![].into()).encode()
                ..Key::TxnUpdate(self.id + 1, vec![].into()).encode(),
        ));
        
        for item in scan {
            let (k,_) = item?;
            match Key::decode(&k)? {
                Key::TxnUpdate(_, key) => {
                    // 把update 的key删除 已经不需要了
                    store.delete(&k)?;
                    roallback.push(key.into_owned());
                },
                k => {
                    return Err(Error::Mvcc(format!("expect get txnUpdate key get : {:?}",k)))
                },
            };
        }
        store.flush()?;
        return Ok(roallback)
    }

    /// 下面的操作都是recored相关的操作 所以获得的都应该是record
    /// 删除key数据
    pub fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.write(key, None)
    }

    /// 得到一个key
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let store = self.store.read()?;
        //   从0版本到当前版本 获取
        let scan = store.scan(MyRange::new(
            Key::Record(key.into(), 0).encode()..Key::Record(key.into(), self.id).encode(),
        ));

        // 开始寻找我们需要的
        for item in scan.into_iter().rev() {
            let (k, v) = item?;
            // 将key 解码
            match Key::decode(&k)? {
                Key::Record(_, version) => {
                    if self.snapshot.is_visible(version) {
                        return deserialize(&v);
                    }
                }
                k => {
                    return Err(Error::Encoding(format!(
                        "expect a key recored but get {:?}",
                        k
                    )))
                }
            }
        }
        // 没有就是none
        Ok(None)
    }

    /// 根据范围获得多个数据
    pub fn scan(&self, range: impl RangeBounds<Vec<u8>>) -> Result<super::Scan> {
        // 重新设置一下start end 因为我们的record还包括version
        let start = match range.start_bound() {
            Bound::Excluded(k) => Bound::Excluded(Key::Record(k.into(), std::u64::MAX).encode()),
            Bound::Included(k) => Bound::Included(Key::Record(k.into(), 0).encode()),
            Bound::Unbounded => Bound::Included(Key::Record(vec![].into(), 0).encode()),
        };
        let end = match range.end_bound() {
            Bound::Excluded(k) => Bound::Excluded(Key::Record(k.into(), 0).encode()),
            Bound::Included(k) => Bound::Included(Key::Record(k.into(), std::u64::MAX).encode()),
            Bound::Unbounded => Bound::Unbounded,
        };

        let scan = self.store.read()?.scan(MyRange::new((start, end)));
        Ok(Box::new(MvccScan::new(scan, self.snapshot.clone())))
    }

    /// 根据前缀获取多个数据 (k,v)
    /// end 就是根据start的字节 + 1
    pub fn scan_prefix(&self, prefix: &[u8]) -> Result<super::Scan> {
        if prefix.len() == 0 {
            return Err(Error::Internal("Scan prefix cannot be empty".to_string()));
        }
        let start = prefix.to_vec();
        let mut end = start.clone();
        for i in (0..end.len()).rev() {
            match end[i] {
                0xff => {
                    end[i] = 0x00;
                    continue;
                }
                v => {
                    end[i] = v + 1;
                    break;
                }
            }
        }
        self.scan(start..end)
    }

    /// 设置key val
    pub fn set(&mut self, key: &[u8], value: Vec<u8>) -> Result<()> {
        self.write(key, Some(value))
    }

    /// 写记录
    fn write(&self, key: &[u8], value: Option<Vec<u8>>) -> Result<()> {
        if !self.mode.mutable() {
            return Err(Error::Mvcc("unwritable mvcc mode".to_string()));
        }
        let mut session = self.store.write()?;

        // 得到当前不可见的事务id最小值 没有就是 当前id+1
        let min = self
            .snapshot
            .invisible
            .iter()
            .min()
            .cloned()
            .unwrap_or(self.id + 1);
        let mut scan = session
            .scan(MyRange::new(
                // 找到记录
                Key::Record(key.into(), min).encode()
                    ..=Key::Record(key.into(), std::u64::MAX).encode(),
            ))
            .rev();

        // 查询一下当前的记录是否可见
        // 但凡有一个不可见的 就不能操作
        while let Some((k, _)) = scan.next().transpose()? {
            match Key::decode(&k)? {
                Key::Record(_, version) => {
                    if !self.snapshot.is_visible(version) {
                        return Err(Error::Mvcc("record cannot be write".to_string()));
                    }
                }
                k => {
                    return Err(Error::Internal(format!(
                        "Expected Txn::Record, got {:?}",
                        k
                    )))
                }
            };
        }
        std::mem::drop(scan);

        // 设置key  并设置version 为当前事务的id
        let key = Key::Record(key.into(), self.id).encode();
        let update = Key::TxnUpdate(self.id, (&key).into()).encode();
        // 设置update 这里是为了方便后续roallback
        session.set(&update, vec![])?;
        session.set(&key, serialize(&value)?)
    }
}

/// 形成快照，用来查看哪些数据版本在当前事务下可见
#[derive(Clone, SerializeDerive, DeserializeDerive)]
struct Snapshot {
    /// 当前的事务id
    version: u64,
    /// 事务启动的时候当前的活跃事务id
    invisible: HashSet<u64>,
}

impl Snapshot {
    fn new(version: u64, invisible: HashSet<u64>) -> Self {
        Self { version, invisible }
    }

    /// 传入的版本号对应的数据是否可见
    pub fn is_visible(&self, version: u64) -> bool {
        // 如果小于当前的版本号，并且不在invisible中 就是可见
        version <= self.version && !self.invisible.contains(&version)
    }

    /// 恢复当前活跃的事务id
    fn restore(session: &RwLockReadGuard<Box<dyn Store>>, version: u64) -> Result<Self> {
        match session.get(&Key::TxnSnapshot(version).encode())? {
            Some(ref v) => Ok(Self {
                version,
                invisible: deserialize(v)?,
            }),
            None => Err(Error::Internal(format!(
                "version of snapshot not found {}",
                version
            ))),
        }
    }
}

#[derive(Debug)]
enum Key<'a> {
    /// 获取下一个事务
    TxnNext,
    /// Active txn markers, containing the mode. Used to detect concurrent txns, and to resume.
    TxnActive(u64),
    /// Txn snapshot, containing concurrent active txns at start of txn.
    TxnSnapshot(u64),
    /// 更新 标记 用于rollback
    /// (version,record_key)
    TxnUpdate(u64, Cow<'a, [u8]>),
    /// 记录的key和version
    Record(Cow<'a, [u8]>, u64),
    /// 保存元数据的key
    Metadata(Cow<'a, [u8]>),
}

impl<'a> Key<'a> {
    /// 编码一个key
    fn encode(self) -> Vec<u8> {
        use encoding::*;
        match self {
            Self::TxnNext => vec![0x01],
            Self::TxnActive(id) => [&[0x02][..], &encode_u64(id)].concat(),
            Self::TxnSnapshot(version) => [&[0x03][..], &encode_u64(version)].concat(),
            Self::TxnUpdate(id, key) => {
                [&[0x04][..], &encode_u64(id), &encode_bytes(&key)].concat()
            }
            Self::Metadata(key) => [&[0x05][..], &encode_bytes(&key)].concat(),
            Self::Record(key, version) => {
                [&[0xff][..], &encode_bytes(&key), &encode_u64(version)].concat()
            }
        }
    }

    /// 解码
    fn decode(mut bytes: &[u8]) -> Result<Self> {
        use encoding::*;
        let bytes = &mut bytes;
        let key = match take_byte(bytes)? {
            0x01 => Self::TxnNext,
            0x02 => Self::TxnActive(take_u64(bytes)?),
            0x03 => Self::TxnSnapshot(take_u64(bytes)?),
            0x04 => Self::TxnUpdate(take_u64(bytes)?, take_bytes(bytes)?.into()),
            0x05 => Self::Metadata(take_bytes(bytes)?.into()),
            0xff => Self::Record(take_bytes(bytes)?.into(), take_u64(bytes)?),
            b => {
                return Err(Error::Internal(format!(
                    "get unknown MVCC key prefix {:x?}",
                    b
                )))
            }
        };
        if !bytes.is_empty() {
            return Err(Error::Internal(
                "get unexpected data at end of key".to_string(),
            ));
        }
        Ok(key)
    }
}

struct MvccScan {
    /// 这个scan是最原始的scan 我们需要进行包装 来解决隔离性问题，因为同一个key会对应不同的版本
    scan: Peekable<super::Scan>,
    /// 保留next_back上一个得到的item 这样才能对比version
    next_back_seen: Option<Vec<u8>>,
}
impl MvccScan {
    /// 创建scan
    fn new(mut scan: super::Scan, snapshot: Snapshot) -> Self {
        // 我们首先过滤掉不可见的版本
        // 这里的k-v会包含多个版本，我们需要的是最新的版本
        scan = Box::new(scan.filter_map(move |r| {
            r.and_then(|(k, v)| match Key::decode(&k)? {
                Key::Record(_, version) if !snapshot.is_visible(version) => Ok(None),
                Key::Record(key, _) => Ok(Some((key.into_owned(), v))),
                k => Err(Error::Internal(format!("Expected Record, got {:?}", k))),
            })
            .transpose()
        }));
        Self {
            scan: scan.peekable(),
            next_back_seen: None,
        }
    }

    fn try_next(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        while let Some((key, value)) = self.scan.next().transpose()? {
            // 只返回最后一个版本，先获取当前一个
            // 然后peek下一个
            if match self.scan.peek() {
                // 如果key不一样 说明没问题
                Some(Ok((peek_key, _))) if *peek_key != key => true,
                // 一样就下一位
                Some(Ok(_)) => false,
                // 出现错误了就返回错误
                Some(Err(err)) => return Err(err.clone()),
                // 都没下一个了 也没问题
                None => true,
            } {
                // 只返回没有删除的item
                // 这里的value需要解码 因为解码的结果是一个option
                if let Some(value) = deserialize(&value)? {
                    return Ok(Some((key, value)));
                }
            }
        }
        Ok(None)
    }

    fn try_next_back(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        while let Some((key, value)) = self.scan.next_back().transpose()? {
            // 返回最后一个版本 需要查看上一个的item
            if match &self.next_back_seen {
                // 如果之前没有就true
                None => true,
                // 如果之前的key不等于当前的key 也是true
                Some(seen_key) if *seen_key != key => true,
                Some(_) => false,
            } {
                self.next_back_seen = Some(key.clone());
                // 返回没有被删除的item
                if let Some(value) = deserialize(&value)? {
                    return Ok(Some((key, value)));
                }
            }
        }
        Ok(None)
    }
}

impl Iterator for MvccScan {
    type Item = Result<(Vec<u8>, Vec<u8>)>;
    fn next(&mut self) -> Option<Self::Item> {
        self.try_next().transpose()
    }
}

impl DoubleEndedIterator for MvccScan {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.try_next_back().transpose()
    }
}


fn serialize<V: Serialize>(value: &V) -> Result<Vec<u8>> {
    Ok(bincode::serialize(value)?)
}

fn deserialize<'a, V: Deserialize<'a>>(bytes: &'a [u8]) -> Result<V> {
    Ok(bincode::deserialize(bytes)?)
}
