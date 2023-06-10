use crate::errors::Result;

use std::collections::BTreeMap;
use std::fmt::Display;
use super::{MyRange, Scan, SqlStore};

pub struct BtreeStore {
    data: BTreeMap<Vec<u8>, Vec<u8>>,
}

impl BtreeStore {
    pub fn new() -> Self {
        Self {
            data: BTreeMap::new(),
        }
    }
}

impl Display for BtreeStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "BtreeStore")
    }
}

impl SqlStore for BtreeStore {
    fn flush(&mut self) -> Result<()> {
        Ok(())
    }

    fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.data.remove(key);
        Ok(())
    }

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(self.data.get(key).cloned())
    }

    fn scan(&self, range: MyRange) -> Scan {
        Box::new(
            self.data
                .range(range)
                .map(|(k, v)| Ok((k.clone(), v.clone())))
                .collect::<Vec<_>>()
                .into_iter(),
        )
    }

    fn set(&mut self, key: &[u8], value: Vec<u8>) -> Result<()> {
        let r = self.data.insert(key.to_vec(), value);
        Ok(())
    }
}
