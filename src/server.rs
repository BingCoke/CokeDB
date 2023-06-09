use crate::{
    errors::{Error, *},
    sql::{
        self,
        engine::{
            kv::{KvTransaction, KV},
            Engine, SqlSession, Transaction,
        },
        schema::Catalog,
    },
    storage::kv::mvcc::{Mode, Status},
};
use futures_util::{future::ok, SinkExt, StreamExt};
use log::{error, info, debug};
use serde_derive::{Deserialize, Serialize};
use tokio::net::{TcpListener, TcpStream};
use tokio_stream::wrappers::TcpListenerStream;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

use crate::{
    sql::{
        execution::{ResultSet, Row},
        Table,
    },
    storage::kv::SqlStore,
};

use crate::storage::kv::mvcc::MVCC;

pub struct Server {
    sql_listener: Option<TcpListener>,
    sql_eninge: KV,
    sql_addr: String
}

impl Server {
    // 创建一个server实例
    pub fn new(sql_addr: &str, sql_store: Box<dyn SqlStore>) -> Self {
        // create mvcc
        let mvcc = MVCC::new(sql_store);
        let kv_sql_engine = KV::new(mvcc);
        Self {
            sql_listener: None,
            sql_eninge: kv_sql_engine,
            sql_addr: sql_addr.to_string()
        }
    }

    pub async fn server(mut self) -> Result<()> {
        let sql_listener = TcpListener::bind(&self.sql_addr).await?;
        self.sql_listener = Some(sql_listener);
        self.handle_sql_request().await?;
        Ok(())
    }

    async fn handle_sql_request(self) -> Result<()> {
        if let Some(sql_listener) = self.sql_listener {
            let mut listener = TcpListenerStream::new(sql_listener);
            while let Some(listener) = listener.next().await.transpose()? {
                let addr = listener.peer_addr();
                info!("get client connection {:?}", addr);
                let session = Session::new(self.sql_eninge.clone(), listener)?;

                tokio::spawn(async {
                    match session.serve().await {
                        Ok(_) => {
                            info!("disconnect")
                        }
                        Err(e) => {
                            error!("get error {}", e)
                        }
                    }
                });
            }
        } else {
            return Err(Error::IO("no get a sql_listener".to_string()));
        }
        Ok(())
    }
}

pub struct Session {
    // sql engine
    engine: sql::engine::kv::KV,
    sql_session: SqlSession<KV>,
    socket: Option<TcpStream>,
}

impl Session {
    pub fn new(engine: KV, socket: TcpStream) -> Result<Self> {
        let socket = Some(socket);
        let sql_session = engine.session()?;
        Ok(Self {
            engine,
            sql_session,
            socket,
        })
    }

    pub async fn serve(mut self) -> Result<()> {
        /* Framed结构体是将字节流转换为高级对象流的方便方法。它有两个参数：底层流和编解码器，用于序列化和反序列化在流中发送的对象。
        在这种情况下，底层流使用Framed::new函数创建，它需要一个socket（可能是TCP或UDP套接字）和一个LengthDelimitedCodec。
        LengthDelimitedCodec是由tokio_util库提供的编解码器，它通过在每个对象的前面添加其字节长度来对数据进行帧处理。
        然后使用tokio_serde::formats::Bincode编解码器对对象进行序列化和反序列化。Bincode是用于Rust值的二进制序列化格式. */
        let socket = self.socket.take().unwrap();

        let mut stream = tokio_serde::Framed::new(
            Framed::new(socket, LengthDelimitedCodec::new()),
            tokio_serde::formats::Bincode::default(),
        );

        while let Some(req) = stream.next().await {
            let req = req?;
            let response = self.handle_request(req);
            stream.send(response).await?;
        }
        Ok(())
    }

    pub fn handle_request(&mut self, req: Request) -> Result<Response> {
     
        // 根据request不同类型进行不同的执行
        let r = match req {
            Request::Execute(sql) => {
                let r = self.sql_session.execute(&sql)?;
                Response::Execute(r)
            }
            Request::GetTable(s) => {
                let r = self
                    .sql_session
                    .with_txn(Mode::ReadOnly, |txn| txn.must_read_table(&s))?;
                Response::GetTable(r)
            }
            Request::ListTables => {
                let r = self
                    .sql_session
                    .with_txn(Mode::ReadOnly, |txn| {
                        let tables = txn.scan_tables();
                        tables
                    })?
                    .into_iter()
                    .map(|t| t.name)
                    .collect();
                Response::ListTables(r)
            }
            Request::Status => Response::Status(self.engine.get_statue()?),
        };
        Ok(r)
    }
}

/// client Request
#[derive(Debug, Serialize, Deserialize)]
pub enum Request {
    Execute(String),
    GetTable(String),
    ListTables,
    Status,
}

/// server Response
#[derive(Debug, Serialize, Deserialize)]
pub enum Response {
    Execute(ResultSet),
    Row(Option<Row>),
    GetTable(Table),
    ListTables(Vec<String>),
    Status(Status),
}
