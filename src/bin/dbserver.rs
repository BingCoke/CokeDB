use clap::{arg, command, Parser};
use coke_db::{errors::*, storage::kv::b_tree::BtreeStore, server::Server};
use config::File;
use log::{debug, info};
use serde_derive::Deserialize;
#[tokio::main]
pub async fn main() -> Result<()> {
    // parse and get config
    let dbSer = DbSer::parse();
    println!("db: {:?}", dbSer);
    let config = Config::new(&dbSer.config)?;
    println!("{:?}", config);
    debug!("get config : \n {:?}", config);

    // init log
    let mut logconfig = simplelog::ConfigBuilder::new();
    let loglevel = config.log_level.parse::<simplelog::LevelFilter>()?;
    logconfig.add_filter_allow_str("coke_db");
    simplelog::SimpleLogger::init(loglevel, logconfig.build())?;

    //let data_dir = std::path::Path::new(&config.data_dir);

    let store = BtreeStore::new();
    let server = Server::new(&config.listen_sql_addr, Box::new(store));
    info!("server will listen on {}",config.listen_sql_addr);
    debug!("server id is {}",config.id);
    server.server().await?;
    Ok(())
}



#[derive(Parser)]
#[command(name = "dbServer")]
#[command(author = "bingcoke")]
#[command(version = "1.0")]
#[command(about = "this is db server")]
#[derive(Debug)]
struct DbSer {
    #[arg(short, long)]
    #[arg(default_value_t = default_file_path())]
    #[arg(help = "config file path")]
    config: String,
}

fn default_file_path() -> String {
    let file_path = std::env::var_os("HOME")
        .map(|home| {
            std::path::Path::new(&home)
                .join(".config")
                .join("coke_db")
                .join("coke_db.yml")
        })
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    file_path
}



#[derive(Debug, Deserialize)]
struct Config {
    id: String,
    listen_sql_addr: String,
    log_level: String,
    data_dir: String,
}

impl Config {
    fn new(config: &str) -> Result<Self> {
        let c = config::Config::builder()
            .set_default("id", "coke_db")?
            .set_default("listen_sql_addr", "0.0.0.0:9653")?
            .set_default("log_level", "info")?
            .set_default("data_dir", "")?
            .add_source(File::with_name(config))
            .build()?;
        Ok(c.try_deserialize()?)
    }
}
