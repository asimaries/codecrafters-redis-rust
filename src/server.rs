use std::sync::Arc;

use tokio::{
    fs::File,
    io::{AsyncReadExt, BufReader},
    net::{TcpListener, TcpStream},
    sync::RwLock,
};

use crate::{
    config::Config,
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
        let db = Arc::new(RwLock::new(Storage::new()));
        loop {
            let stream = self.listener.accept().await;
            match stream {
                Ok((stream, _)) => {
                    let db_arc = Arc::clone(&db);
                    tokio::spawn(async move {
                        let _ = Self::handle_client(stream, db_arc).await;
                    });
                }
                Err(e) => {
                    eprintln!("error: {}", e);
                }
            };
        }
    }

    async fn handle_client(stream: TcpStream, db: Arc<RwLock<Storage>>) -> Result<(), RespError> {
        let mut handler = RespHandler::new(stream);
        let config = Config::new();
        loop {
            let value = handler.read_value().await.unwrap();
            let response = if let Some(v) = value {
                let (command, args) = Self::extract_command(v).unwrap();
                match command.to_lowercase().as_str() {
                    "ping" => Value::SimpleString("PONG".to_owned()),
                    "echo" => args.first().unwrap().clone().to_owned(),
                    "set" => {
                        let key = Self::unpack_bulk_string(args[0].clone())?;
                        let value = Self::unpack_bulk_string(args[1].clone())?;
                        let mut ttl: Option<usize> = None;
                        if args.len() > 3 {
                            ttl = Self::unpack_bulk_string(args[3].to_owned())
                                .ok()
                                .and_then(|s| s.parse::<usize>().ok());
                        };
                        db.write().await.set(key, value, ttl).await
                    }
                    "get" => {
                        let a = db
                            .read()
                            .await
                            .get(Self::unpack_bulk_string(args[0].clone())?)
                            .await;
                        a
                    }
                    "config" => {
                        if args.len() > 1 {
                            let arg = Self::unpack_bulk_string(args[0].clone())?.to_lowercase();
                            match arg.as_str() {
                                "get" => {
                                    let param = Self::unpack_bulk_string(args[1].clone())?;
                                    let value = match param.as_str() {
                                        "dir" => Value::BulkString(
                                            config.dir.clone().unwrap_or_default(),
                                        ),
                                        "dbfilename" => Value::BulkString(
                                            config.dbfilename.clone().unwrap_or_default(),
                                        ),
                                        _ => Value::SimpleError("Unknown arguments".to_owned()),
                                    };

                                    Value::Array(vec![Value::BulkString(param), value])
                                }
                                _ => Value::SimpleString("OK".to_owned()),
                            }
                        } else {
                            Value::SimpleError("Invalid number of arguments".to_owned())
                        }
                    }
                    "keys" => {
                        let mut path = config.dir.clone().unwrap_or_default();
                        let dbfilename = config.dbfilename.clone().unwrap_or_default();
                        path.push_str("/");
                        path.push_str(&dbfilename);
                        let file = File::open(&path).await;
                        let key = match file {
                            Ok(file) => {
                                let mut buffer: [u8; 1024] = [0; 1024];
                                let mut reader = BufReader::new(file);
                                let buf_size = reader.read(&mut buffer).await;
                                if !(buf_size.unwrap() > 0) {
                                    return Err(RespError::Other(format!("Cannot Handle command")));
                                }

                                let fb_pos = buffer.iter().position(|&b| b == 0xfb).unwrap();
                                let mut pos = fb_pos + 4;
                                let len = buffer[pos] as usize;
                                pos += 1;
                                let key = &buffer[pos..(pos + len)];
                                let parse = String::from_utf8_lossy(key).parse().unwrap();
                                parse
                            }
                            Err(e) => format!("Cannot Handle command\n{}", e),
                        };
                        Value::Array(vec![Value::BulkString(key)])
                    }
                    _ => Value::SimpleError(format!("Cannot Handle command {}", command)),
                }
            } else {
                return Ok(());
            };
            println!("{:?}", response);
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
