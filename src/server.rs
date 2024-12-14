use std::{collections::HashMap, time::Instant};

use tokio::net::{TcpListener, TcpStream};

use crate::{
    resp::{
        resp::{parse_message, RespHandler, RespParser, Value},
        RespError,
    },
    storage::Storage,
};

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

    async fn handle_client(stream: TcpStream) -> Result<(), RespError> {
        let mut handler = RespHandler::new(stream);
        let mut db = Storage::new();
        loop {
            let value = handler.read_value().await.unwrap();
            println!("{:?}", value);
            let response = if let Some(v) = value {
                let (command, args) = Self::extract_command(v).unwrap();
                match command.to_lowercase().as_str() {
                    "ping" => Value::SimpleString("PONG".to_owned()),
                    "echo" => args.first().unwrap().clone().to_owned(),
                    "set" => {
                        let key = Self::unpack_bulk_string(args[0].clone())?;
                        let value = Self::unpack_bulk_string(args[1].clone())?;
                        let mut expires = 0;
                        if args.len() > 2 {
                            expires = Self::unpack_bulk_string(args[3].to_owned())?
                                .parse::<usize>()
                                .unwrap_or(0);
                        };
                        db.set(key, value, expires)
                    }
                    "get" => {
                        let a = db.get(Self::unpack_bulk_string(args[0].clone())?);
                        println!("{:?}", a);
                        a
                    }
                    _ => Value::SimpleError(format!("Cannot Handle command {}", command)),
                }
            } else {
                return Ok(());
            };
            handler.write_value(response).await.unwrap();
            ()
        }
    }
    fn extract_command(value: Value) -> Result<(String, Vec<Value>), RespError> {
        match value {
            Value::Array(a) => Ok((
                Self::unpack_bulk_string(a.first().unwrap().clone())?,
                a.into_iter().skip(1).collect(),
            )),
            _ => Err(RespError::Other(format!("Unexpected Command format"))),
        }
    }

    fn unpack_bulk_string(value: Value) -> Result<String, RespError> {
        match value {
            Value::BulkString(s) => Ok(s),
            _ => Err(RespError::Other(format!(
                "Expected Command to be a Bulk String"
            ))),
        }
    }
}
