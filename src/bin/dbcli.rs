use std::ops::RangeInclusive;

use clap::{arg, Parser};
use coke_db::client::{self, Client};
use coke_db::errors::*;
use coke_db::sql::execution::ResultSet;
use coke_db::sql::parser::laxer::{Laxer, Token};
use coke_db::storage::kv::mvcc::Mode;
use futures_util::future::ok;
use rustyline::history::FileHistory;
use rustyline::validate::{ValidationContext, ValidationResult, Validator};
use rustyline::Editor;
use rustyline_derive::{Completer, Helper, Highlighter, Hinter};

use std::result::Result as R;

#[tokio::main]
async fn main() -> Result<()> {
    let mut logconfig = simplelog::ConfigBuilder::new();
    let loglevel = simplelog::LevelFilter::Info;
    logconfig.add_filter_allow_str("coke_db");
    simplelog::SimpleLogger::init(loglevel, logconfig.build())?;

    let c1 = DbCli::parse();

    println!("try to connect {}:{}", c1.host, c1.port);
    println!("use try to input \"!h\" to get help");
    let client = Client::new(&c1.host, c1.port).await?;

    run(client).await?;

    Ok(())
}

#[derive(Parser)]
#[command(name = "dbcli")]
#[command(author = "bingcoke")]
#[command(version = "1.0")]
#[command(about = "this is a dbcli to connect CokeDB")]
struct DbCli {
    #[arg(long)]
    #[arg(short = 'H')]
    #[arg(default_value_t = host_default())]
    #[arg()]
    host: String,
    #[arg(short, long)]
    #[arg(value_parser = port_in_range)]
    #[arg(default_value_t = 9653)]
    #[arg(help = "port column headers")]
    port: u16,
}

struct Cli {
    client: Client,
    editor: Editor<InputValidator, FileHistory>,
}
impl Cli {
    fn get_prompt(&self) -> Result<String> {
        let propmt = match self.client.txn() {
            Some((id, _)) => {
                format!("coke_db: {} >> ", id)
            }
            None => "coke_db >> ".to_string(),
        };
        Ok(propmt)
    }

    async fn execute(&self, query: &str) -> Result<()> {
        if query.starts_with("!") {
            let mut command = query.split_whitespace();
            let mut getnext = || -> R<&str, Error> {
                command
                    .next()
                    .ok_or_else(|| Error::Parse("get unexpect end".to_string()))
            };
            match getnext()? {
                "!h" | "!help" => {
                    println!(
                        "
ctrl+c => quit
!tables => get all tables
!table <table> => get table
!status => get status
"
                    )
                }
                "!tables" => {
                    let tables = self.client.list_tables().await?;
                    println!("show tables");
                    for table in tables {
                        println!("{table}")
                    }
                }
                "!table" => {
                    let table = getnext()?;
                    let table = self.client.get_table(table).await?;
                    println!("get table {:#?}", table);
                }
                "!status" => {
                    let status = self.client.get_status().await?;
                    println!("server status {:#?}", status);
                }
                de => {}
            }
            Ok(())
        } else if !query.is_empty() {
            match self.client.execute(query).await? {
                ResultSet::Begin { id, mode } => match mode {
                    Mode::ReadWrite => println!("Began transaction {}", id),
                    Mode::ReadOnly => println!("Began read-only transaction {}", id),
                    Mode::Snapshot { version, .. } => println!(
                        "Began read-only transaction {} in snapshot at version {}",
                        id, version
                    ),
                },
                ResultSet::Commit { id } => println!("Committed transaction {}", id),
                ResultSet::Rollback { id } => println!("Rolled back transaction {}", id),
                ResultSet::Create { count } => println!("Created {} rows", count),
                ResultSet::Delete { count } => println!("Deleted {} rows", count),
                ResultSet::Update { count } => println!("Updated {} rows", count),
                ResultSet::CreateTable { name } => println!("Created table {}", name),
                ResultSet::DropTable { name } => println!("Dropped table {}", name),
                ResultSet::Explain(plan) => println!("{}", plan.to_string()),
                ResultSet::Query { columns, rows } => {
                    println!(
                        "{}",
                        columns
                            .iter()
                            .map(|c| c.as_deref().unwrap_or("?"))
                            .collect::<Vec<_>>()
                            .join("|")
                    );
                    for row in rows.into_iter() {
                        println!(
                            "{}",
                            row.into_iter()
                                .map(|v| format!("{}", v))
                                .collect::<Vec<_>>()
                                .join("|")
                        );
                    }
                }
            }
            Ok(())
        } else {
            Ok(())
        }
    }
}

const PORT_RANGE: RangeInclusive<usize> = 1..=65535;

async fn run(client: Client) -> Result<()> {
    let mut editor: Editor<InputValidator, _> = Editor::new()?;
    let history_path =
        std::env::var_os("HOME").map(|home| std::path::Path::new(&home).join(".sql_history"));
    if let Some(history) = &history_path {
        let _ = editor.load_history(history);
    }
    editor.set_helper(Some(InputValidator {}));

    let mut cli = Cli { client, editor };

    let status = cli.client.get_status().await?;
    println!("{:?}", status);

    loop {
        let propmt = cli.get_prompt()?;

        let input = match cli.editor.readline(&propmt) {
            Ok(input) => {
                cli.editor.add_history_entry(&input)?;
                Ok(input)
            }
            Err(err) => Err(err),
        };
        let input = if input.is_err() { break } else { input? };

        match cli.execute(&input).await {
            Ok(()) => {}
            error @ Err(Error::Internal(_)) => return error,
            Err(error) => println!("Error: {}", error.to_string()),
        }
    }

    if let Some(history) = &history_path {
        cli.editor.save_history(&history)?;
    }
    Ok(())
}

fn host_default() -> String {
    "127.0.0.1".to_string()
}

fn port_in_range(s: &str) -> R<u16, String> {
    let port: usize = s
        .parse()
        .map_err(|_| format!("`{s}` isn't a port number"))?;
    if PORT_RANGE.contains(&port) {
        Ok(port as u16)
    } else {
        Err(format!("{}", "port no range in 1-65535"))
    }
}

#[derive(Completer, Helper, Highlighter, Hinter)]
struct InputValidator;

// 检查是否合法
impl Validator for InputValidator {
    fn validate(&self, ctx: &mut ValidationContext) -> rustyline::Result<ValidationResult> {
        let input = ctx.input();

        // 如果是空行或者! 就没问题
        if input.is_empty() || input.starts_with('!') || input == ";" {
            return Ok(ValidationResult::Valid(None));
        }

        for result in Laxer::new(ctx.input()) {
            match result {
                Ok(Token::Semicolon) => return Ok(ValidationResult::Valid(None)),
                Err(_) => return Ok(ValidationResult::Valid(None)),
                _ => {}
            }
        }
        // 如果上面都没有返回就说明语句没有结束
        Ok(ValidationResult::Incomplete)
    }

    fn validate_while_typing(&self) -> bool {
        false
    }
}
