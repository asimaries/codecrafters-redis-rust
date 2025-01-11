use std::{
    sync::Arc,
    time::{Duration, SystemTime},
};

use tokio::{
    fs::File,
    io::{AsyncReadExt, BufReader},
    net::{TcpListener, TcpStream},
    sync::RwLock,
    time,
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

    pub async fn run(&mut self, config: Arc<Config>) {
        let db = Arc::new(RwLock::new(Storage::new()));
        if config.has_rdb() {
            let db_clone = Arc::clone(&db);
            let config_clone = Arc::clone(&config);
            tokio::spawn(async move {
                let _ = db_clone.write().await.load_from_rdb(config_clone).await;
            });
            let config_clone = Arc::clone(&config);
            let db_save_storage = Arc::clone(&db);
            tokio::spawn(async move {
                let mut interval_time = time::interval(Duration::from_secs(5));
                loop {
                    interval_time.tick().await;
                    println!("Saving to rdb");
                    let save_db = db_save_storage
                        .write()
                        .await
                        .save_to_rdb(&config_clone)
                        .await;
                    match save_db {
                        Ok(_) => {}
                        Err(e) => {
                            return e;
                        }
                    }
                }
            });
        }

        loop {
            let stream = self.listener.accept().await;
            match stream {
                Ok((stream, _)) => {
                    let db_clone = Arc::clone(&db);
                    let config_clone = Arc::clone(&config);

                    tokio::spawn(async move {
                        let _ = Self::handle_client(stream, db_clone, config_clone).await;
                    });
                }
                Err(e) => {
                    eprintln!("error: {}", e);
                }
            };
        }
    }

    async fn handle_client(
        stream: TcpStream,
        db: Arc<RwLock<Storage>>,
        config: Arc<Config>,
    ) -> Result<(), RespError> {
        let mut handler = RespHandler::new(stream);
        loop {
            let value = handler.read_value().await.unwrap();
            let response = if let Some(v) = value {
                let (command, args) = Self::extract_command(v).unwrap();
                match command.to_lowercase().as_str() {
                    "ping" => Value::SimpleString("PONG".to_owned()),
                    "echo" => args.first().unwrap().clone().to_owned(),
                    "set" => {
                        let key = Self::unpack_bulk_string(args.get(0).unwrap().clone())?;
                        let value = Self::unpack_bulk_string(args.get(1).unwrap().clone())?;
                        let mut ttl: Option<SystemTime> = None;
                        if args.len() > 3 {
                            let units = Self::unpack_bulk_string(args.get(2).unwrap().clone())?
                                .to_lowercase();
                            let amount =
                                match Self::unpack_bulk_string(args.get(3).unwrap().clone()) {
                                    Ok(str) => str.parse::<u64>().map_err(|e| {
                                        RespError::Other(format!(
                                            "Unable to parse RDB file\n{:?}",
                                            e
                                        ))
                                    })?,
                                    Err(_) => {
                                        return Err(RespError::Other(format!("unexpected expiry")))
                                    }
                                };
                            ttl = match units.as_str() {
                                "px" => Some(SystemTime::now() + Duration::from_millis(amount)),
                                "ex" => Some(SystemTime::now() + Duration::from_secs(amount)),
                                _ => None,
                            }
                        };
                        db.write().await.set(key, value, ttl).await
                    }
                    "get" => {
                        let a = db
                            .read()
                            .await
                            .get(Self::unpack_bulk_string(args.first().unwrap().clone())?)
                            .await;
                        a
                    }
                    "config" => {
                        if args.len() > 1 {
                            let arg = Self::unpack_bulk_string(args.first().unwrap().clone())?
                                .to_lowercase();
                            match arg.as_str() {
                                "get" => {
                                    let param =
                                        Self::unpack_bulk_string(args.get(1).unwrap().clone())?;
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
                        let pattern = if args.len() > 0 {
                            Self::unpack_bulk_string(args.first().unwrap().clone())?.to_lowercase()
                        } else {
                            "*".to_owned()
                        };
                        db.read().await.keys(pattern)
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
