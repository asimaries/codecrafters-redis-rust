use tokio::net::{TcpListener, TcpStream};

use crate::resp::{RespHandler, Value};
use anyhow::{anyhow, Result};

pub struct Server {
    listener: TcpListener,
}
impl Server {
    // Create and returns Server instance with TcpListener
    pub fn new(listener: TcpListener) -> Self {
        Self { listener }
    }

    pub async fn run(&mut self) {
        loop {
            let stream = self.listener.accept().await;
            match stream {
                Ok((stream, _)) => {
                    tokio::spawn(async move {
                        Self::handle_client(stream).await;
                    });
                }
                Err(e) => {
                    println!("error: {}", e);
                }
            };
        }
    }

    async fn handle_client(stream: TcpStream) {
        let mut handler = RespHandler::new(stream);
        loop {
            let value = handler.read_value().await.unwrap();
            println!("{:?}", value);
            let response = if let Some(v) = value {
                let (command, args) = Self::extract_command(v).unwrap();
                match command.to_lowercase().as_str() {
                    "ping" => Value::SimpleString("PONG".to_owned()),
                    "echo" => args.first().unwrap().clone().to_owned(),
                    _ => panic!("Cannot Handle command {}", command),
                }
            } else {
                break;
            };
            handler.write_value(response).await.unwrap();
        }
    }
    fn extract_command(value: Value) -> Result<(String, Vec<Value>)> {
        match value {
            Value::Array(a) => Ok((
                Self::unpack_bulk_string(a.first().unwrap().clone())?,
                a.into_iter().skip(1).collect(),
            )),
            _ => Err(anyhow::anyhow!("Unexpected Command format")),
        }
    }

    fn unpack_bulk_string(value: Value) -> Result<String> {
        match value {
            Value::BulkString(s) => Ok(s),
            _ => Err(anyhow::anyhow!("Expected Command to be a Bulk String")),
        }
    }
}
