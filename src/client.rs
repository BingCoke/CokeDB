use crate::errors::*;
use crate::server::{Request, Response};
use crate::sql::execution::ResultSet;
use crate::sql::Table;
use crate::storage::kv::mvcc::{Mode, Status};
use futures::future::FutureExt as _;
use futures::sink::SinkExt as _;
use log::debug;
use std::{cell::Cell, sync::Arc};

use futures::stream::TryStreamExt as _;
use futures_util::TryStream;

use std::future::Future;
use std::ops::{Deref, Drop};
use tokio::net::{TcpStream, ToSocketAddrs};
use tokio::sync::{Mutex, MutexGuard};
use tokio_util::codec::{Framed, FramedRead, FramedWrite, LengthDelimitedCodec};

/// 定义一个connection
/// 设置对应的request和response
type Connection = tokio_serde::Framed<
    Framed<TcpStream, LengthDelimitedCodec>,
    Result<Response>,
    Request,
    tokio_serde::formats::Bincode<Result<Response>, Request>,
>;

#[derive(Debug)]
pub struct Client {
    conn: Arc<Mutex<Connection>>,
    txn: Cell<Option<(u64, Mode)>>,
}

impl Client {
    /// Creates a new client
    pub async fn new(host: &str, port: u16) -> Result<Self> {
        Ok(Self {
            conn: Arc::new(Mutex::new(tokio_serde::Framed::new(
                Framed::new(
                    TcpStream::connect((host, port)).await?,
                    LengthDelimitedCodec::new(),
                ),
                tokio_serde::formats::Bincode::default(),
            ))),
            txn: Cell::new(None),
        })
    }

    /// Call a server method
    async fn call(&self, request: Request) -> Result<Response> {
        let mut conn = self.conn.lock().await;
        debug!("send request : {:?}", request);
        conn.send(request).await?;
        debug!("send success");
        match conn.try_next().await? {
            Some(resp) => resp,
            None => Err(Error::Internal("server disconnect".to_string())),
        }
    }

    pub async fn execute(&self, query: &str) -> Result<ResultSet> {
        debug!("try to query {}", query);

        let resultset = match self.call(Request::Execute(query.into())).await? {
            Response::Execute(rs) => rs,
            resp => return Err(Error::Internal(format!("Unexpected response {:?}", resp))),
        };

        debug!("get result {:?}", resultset);

        // if let ResultSet::Query { columns, .. } = resultset {
        //     let mut rows = Vec::new();
        //     let mut conn = self.conn.lock().await;
        //     while let Some(result) = conn.try_next().await? {
        //         match result? {
        //             Response::Row(Some(row)) => rows.push(row),
        //             Response::Row(None) => break,
        //             response => {
        //                 return Err(Error::Internal(format!(
        //                     "Unexpected response {:?}",
        //                     response
        //                 )))
        //             }
        //         }
        //     }
        //     resultset = ResultSet::Query { columns, rows }
        // };

        match &resultset {
            ResultSet::Begin { id, mode } => self.txn.set(Some((*id, *mode))),
            ResultSet::Commit { .. } => self.txn.set(None),
            ResultSet::Rollback { .. } => self.txn.set(None),
            _ => {}
        }
        Ok(resultset)
    }

    ///  获得当前事务的状态
    pub fn txn(&self) -> Option<(u64, Mode)> {
        self.txn.get()
    }

    /// 得到某一个table
    pub async fn get_table(&self, table: &str) -> Result<Table> {
        match self.call(Request::GetTable(table.into())).await? {
            Response::GetTable(t) => Ok(t),
            resp => Err(Error::Executor(format!("Unexpected response: {:?}", resp))),
        }
    }

    /// 得到所有的table
    pub async fn list_tables(&self) -> Result<Vec<String>> {
        match self.call(Request::ListTables).await? {
            Response::ListTables(t) => Ok(t),
            resp => Err(Error::Executor(format!("Unexpected response: {:?}", resp))),
        }
    }

    pub async fn get_status(&self) -> Result<Status> {
        match self.call(Request::Status).await? {
            Response::Status(s) => Ok(s),
            resp => Err(Error::Executor(format!("Unexpected response: {:?}", resp))),
        }
    }
}
