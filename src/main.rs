#![allow(unused_imports)]

mod config;
mod resp;
mod server;
mod storage;

use std::fmt::Error;

use crate::resp::resp::{parse_message, RespHandler, Value};
use server::Server;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    let mut server = Server::new(listener);

    server.run().await;
    Ok(())
}
