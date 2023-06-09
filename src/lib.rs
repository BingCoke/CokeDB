use sql::execution::ResultSet;

pub mod sql;
pub mod storage;
pub mod errors;
pub mod client;
pub mod server;
pub mod util;

/// .
fn hello() {
    let m = "hello";
    let m = Box::new(m);

}
