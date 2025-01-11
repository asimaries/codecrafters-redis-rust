#![allow(unused_imports)]

mod config;
mod resp;
mod server;
mod storage;

use std::{fmt::Error, sync::Arc};

use crate::resp::resp::{parse_message, RespHandler, Value};
use config::Config;
use server::Server;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let config = Arc::new(Config::new());
    let listener = TcpListener::bind(format!("127.0.0.1:{}", config.port.clone().unwrap_or("6379".to_owned()))).await.unwrap();
    let mut server = Server::new(listener);
    server.run(config).await;
    Ok(())
}
