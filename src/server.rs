use std::collections::HashMap;

use tokio::net::{TcpListener, TcpStream};

use crate::resp::{
    resp::{parse_message, RespHandler, RespParser, Value},
    RespError,
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
        let mut storage: HashMap<String, String> = HashMap::new();
        loop {
            let value = handler.read_value().await.unwrap();
            println!("{:?}", value);
            let response = if let Some(v) = value {
                let (command, args) = Self::extract_command(v).unwrap();
                match command.to_lowercase().as_str() {
                    "ping" => Value::SimpleString("PONG".to_owned()),
                    "echo" => args.first().unwrap().clone().to_owned(),
                    "set" => set(
                        &mut storage,
                        args.first().unwrap().clone().serialize(),
                        Self::unpack_bulk_string(args[1].to_owned())
                            .map_err(|e| RespError::Other(format!("{}", e)))?,
                    ),
                    "get" => get(&storage, args.first().unwrap().clone().serialize()),
                    _ => panic!("Cannot Handle command {}", command),
                }
            } else {
                return Ok(());
            };
            handler.write_value(response).await.unwrap();
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
            _ => Err(RespError::Other(format!("Expected Command to be a Bulk String"))),
        }
    }
}

fn set(storage: &mut HashMap<String, String>, key: String, value: String) -> Value {
    storage.insert(key, value);
    Value::SimpleString("OK".to_owned())
}
fn get(storage: &HashMap<String, String>, key: String) -> Value {
    match storage.get(&key) {
        Some(value) => Value::BulkString(value.to_string()),
        None => Value::Null,
    }
}
