use std::{
    collections::HashMap,
    fmt::format,
    path::Path,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use regex::Regex;
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter},
    sync::RwLock,
};

use crate::{
    config::Config,
    resp::{resp::Value, RespError},
};
#[derive(Debug)]
pub(crate) struct Item {
    pub value: Value,
    pub ttl: Option<SystemTime>,
}

pub struct Storage {
    pub storage: HashMap<String, Item>,
}

impl Storage {
    pub fn new() -> Self {
        Self {
            storage: HashMap::new(),
        }
    }

    pub async fn set(&mut self, key: String, value: String, ttl: Option<SystemTime>) -> Value {
        // println!("key {}\nvalue {}\nexpies {:?}", key, value, ttl);
        self.storage.insert(
            key,
            Item {
                value: Value::BulkString(value),
                ttl,
            },
        );
        Value::SimpleString("OK".to_owned())
    }

    pub async fn get(&self, key: String) -> Value {
        match self.storage.get(&key) {
            Some(item) => {
                if let Some(is_expired) = item.ttl {
                    if SystemTime::now() >= is_expired {
                        return Value::Null;
                    }
                };
                return item.value.clone();
            }
            None => Value::Null,
        }
    }
    pub fn keys(&self, pattern: String) -> Value {
        let keys = self.storage.keys().cloned();
        let key_resp = keys
            .filter(|key| key.contains(&pattern.replace("*", "")))
            .map(|key| Value::BulkString(key))
            .collect::<Vec<Value>>();

        Value::Array(key_resp)
    }
    fn unpack_bulk_string(value: Value) -> Result<String, RespError> {
        match value {
            Value::BulkString(s) => Ok(s),
            _ => Err(RespError::Other(format!(
                "Expected Command to be a Bulk String"
            ))),
        }
    }
    pub async fn save_to_rdb(&self, config: &Config) -> Result<(), RespError> {
        if !config.has_rdb() {
            return Err(RespError::Other(format!("no rdb file configured")));
        }

        let map_rdb_err = |e| RespError::Other(format!("Unable to parse RDB file\n{:?}", e));

        let dir = config.dir.as_ref().unwrap();
        let filename = config.dbfilename.as_ref().unwrap();
        let full_path = format!("{}/{}", dir, filename);

        let file = File::create(&full_path).await.map_err(map_rdb_err)?;
        let mut writer = BufWriter::new(file);
        writer.write_all(b"REDIS0006").await.map_err(map_rdb_err)?;

        writer.write_u8(0xFE).await.map_err(map_rdb_err)?;
        writer.write_u32_le(0).await.map_err(map_rdb_err)?;

        writer.write_u8(0xFB).await.map_err(map_rdb_err)?;
        write_length(&mut writer, self.storage.len() as u64)
            .await
            .map_err(|e| RespError::Other(format!("Unable to parse RDB file\n{:?}", e)))?;
        let expires_count = self
            .storage
            .values()
            .filter(|item| item.ttl.is_some())
            .count();
        write_length(&mut writer, expires_count as u64)
            .await
            .map_err(|e| RespError::Other(format!("Unable to parse RDB file\n{:?}", e)))?;

        for (key, item) in &self.storage {
            if let Some(expiry_time) = item.ttl {
                let duration = expiry_time
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or(Duration::from_secs(0));

                writer.write_u8(0xFC).await.map_err(map_rdb_err)?;
                writer
                    .write_u64_le(duration.as_millis() as u64)
                    .await
                    .map_err(map_rdb_err)?;
            }

            writer.write_u8(0x00).await.map_err(map_rdb_err)?;

            write_string(&mut writer, key.as_bytes())
                .await
                .map_err(|e| RespError::Other(format!("Unable to parse RDB file\n{:?}", e)))?;

            write_string(
                &mut writer,
                Self::unpack_bulk_string(item.value.clone())
                    .unwrap()
                    .as_bytes(),
            )
            .await
            .map_err(|e| RespError::Other(format!("Unable to parse RDB file\n{:?}", e)))?;
        }

        writer.write_u8(0xFF).await.map_err(map_rdb_err)?;
        let _f = writer.flush().await;
        Ok(())
    }
    pub async fn load_from_rdb(&mut self, config: Arc<Config>) -> Result<(), RespError> {
        if !config.has_rdb() {
            return Err(RespError::Other("No RDB path configured".to_owned()));
        }
        let parse_err = |e| RespError::Other(format!("Cannot parse version number\n{:?}", e));
        let path = config.get_rdb_path().unwrap();
        if !Path::new(&path).exists() {
            return Ok(());
        }

        let file = File::open(&path).await.map_err(|e| RespError::Io(e))?;
        let mut reader = BufReader::new(file);
        let mut header = [0u8; 9];
        reader
            .read_exact(&mut header)
            .await
            .map_err(|e| RespError::Io(e))?;

        let magic = &header[0..5];
        let version = &header[5..9];

        if &magic != b"REDIS" {
            return Err(RespError::Other("Invalid RDB file".to_owned()));
        }

        let version_str = std::str::from_utf8(version).map_err(parse_err)?;
        let version_num: u32 = version_str
            .parse()
            .map_err(|e| RespError::Other(format!("Cannot parse version number\n{:?}", e)))?;

        if version_num > 11 {
            return Err(RespError::Other(format!(
                "Unsupported RDB version: {}",
                version_num
            )));
        }
        let mut expiry: Option<SystemTime> = None;

        loop {
            let opcode = match reader.read_u8().await {
                Ok(byte) => byte,
                Err(_) => break,
            };

            match opcode {
                0xFA => {
                    let _aux_key = read_string(&mut reader).await?;
                    let _aux_value = read_string(&mut reader).await?;
                    continue;
                }
                0xFB => {
                    let _db_size = read_length(&mut reader).await?;
                    let _expires_size = read_length(&mut reader).await?;

                    continue;
                }
                0xFC => {
                    let expiry_ms = reader.read_u64_le().await.map_err(|e| {
                        RespError::Other(format!("Unable to parse RDB file\n{:?}", e))
                    })?;
                    let expiry_time = UNIX_EPOCH + Duration::from_millis(expiry_ms);
                    expiry = Some(expiry_time);
                    continue;
                }
                0xFD => {
                    let expiry_seconds = reader.read_u32_le().await.map_err(|e| {
                        RespError::Other(format!("Unable to parse RDB file\n{:?}", e))
                    })?;
                    let expiry_time = UNIX_EPOCH + Duration::from_secs(expiry_seconds as u64);
                    expiry = Some(expiry_time);
                    continue;
                }
                0xFE => {
                    let _db_number = reader.read_u32_le().await.map_err(|e| {
                        RespError::Other(format!("Unable to parse RDB file\n{:?}", e))
                    })?;

                    continue;
                }
                0xFF => break,
                value_type => match value_type {
                    0x00 => {
                        let key_bytes = read_string(&mut reader).await.map_err(|e| {
                            RespError::Other(format!("Unable to parse RDB file\n{:?}", e))
                        })?;
                        let value_bytes = read_string(&mut reader).await.map_err(|e| {
                            RespError::Other(format!("Unable to parse RDB file\n{:?}", e))
                        })?;
                        let key = String::from_utf8_lossy(&key_bytes).to_string();
                        let value = String::from_utf8_lossy(&value_bytes).to_string();

                        self.set(key, value, expiry).await;

                        expiry = None;
                    }
                    _ => unimplemented!("Unsupported value type: {}", value_type),
                },
            }
        }

        Ok(())
    }
}

async fn write_length(writer: &mut BufWriter<File>, length: u64) -> Result<(), RespError> {
    let map_rdb_err = |e| RespError::Other(format!("Unable to write RDB file\n{:?}", e));

    if length < 64 {
        writer.write_u8(length as u8).await.map_err(map_rdb_err)?;
    } else if length < 16384 {
        let first_byte = ((length >> 8) as u8) | 0b01000000;
        let second_byte = (length & 0xFF) as u8;
        writer.write_u8(first_byte).await.map_err(map_rdb_err)?;

        writer.write_u8(second_byte).await.map_err(map_rdb_err)?;
    } else {
        writer.write_u8(0b10000000).await.map_err(map_rdb_err)?;

        writer
            .write_u32_le(length as u32)
            .await
            .map_err(map_rdb_err)?;
    }
    Ok(())
}

async fn write_string(writer: &mut BufWriter<File>, bytes: &[u8]) -> Result<(), RespError> {
    let map_rdb_err = |e| RespError::Other(format!("Unable to write RDB file\n{:?}", e));

    let length = bytes.len();

    if length < 64 {
        writer.write_u8(length as u8).await.map_err(map_rdb_err)?;
    } else if length < 16384 {
        let first_byte = ((length >> 8) as u8) | 0b01000000;
        let second_byte = (length & 0xFF) as u8;
        writer.write_u8(first_byte).await.map_err(map_rdb_err)?;
        writer.write_u8(second_byte).await.map_err(map_rdb_err)?;
    } else {
        writer.write_u8(0b10000000).await.map_err(map_rdb_err)?;
        writer
            .write_u32_le(length as u32)
            .await
            .map_err(map_rdb_err)?;
    }

    writer.write_all(bytes).await.map_err(map_rdb_err)?;
    Ok(())
}

async fn read_length(reader: &mut BufReader<File>) -> Result<u64, RespError> {
    let first_byte = reader
        .read_u8()
        .await
        .map_err(|e| RespError::Other(format!("Unable to parse RDB file\n{:?}", e)))?;
    let encoding_type = first_byte >> 6;

    match encoding_type {
        0b00 => {
            let length = (first_byte & 0b00111111) as u64;
            Ok(length)
        }

        0b01 => {
            let second_byte = reader
                .read_u8()
                .await
                .map_err(|e| RespError::Other(format!("Unable to parse RDB file\n{:?}", e)))?;

            let length = ((first_byte & 0b00111111) as u64) << 8 | second_byte as u64;

            Ok(length)
        }

        0b10 => {
            let length = reader
                .read_u32()
                .await
                .map_err(|e| RespError::Other(format!("Unable to parse RDB file\n{:?}", e)))?
                as u64;

            Ok(length)
        }
        0b11 => {
            let special_type = first_byte & 0b00111111;

            Err(RespError::Other(format!(
                "Special encoding not supported: {}",
                special_type
            )))
        }
        _ => Err(RespError::Other(format!("Invalid length encoding"))),
    }
}

async fn read_string(reader: &mut BufReader<File>) -> Result<Vec<u8>, RespError> {
    let first_byte = reader
        .read_u8()
        .await
        .map_err(|e| RespError::Other(format!("Unable to parse RDB file\n{:?}", e)))?;
    let encoding_type = first_byte >> 6;
    // println!("first_byte: {}\nlen: {}", first_byte, encoding_type);

    match encoding_type {
        0b00 | 0b01 | 0b10 => {
            let length = match encoding_type {
                0b00 => (first_byte & 0b00111111) as usize,

                0b01 => {
                    let second_byte = reader.read_u8().await.map_err(|e| {
                        RespError::Other(format!("Unable to parse RDB file\n{:?}", e))
                    })?;
                    (((first_byte & 0b00111111) as usize) << 8) | (second_byte as usize)
                }
                0b10 => {
                    let second_byte = reader.read_u32().await.map_err(|e| {
                        RespError::Other(format!("Unable to parse RDB file\n{:?}", e))
                    })?;
                    second_byte as usize
                }

                _ => unreachable!(),
            };
            let mut buf = vec![0u8; length];
            reader
                .read_exact(&mut buf)
                .await
                .map_err(|e| RespError::Other(format!("Unable to parse RDB file\n{:?}", e)))?;
            Ok(buf)
        }
        0b11 => {
            let special_type = first_byte & 0b00111111;
            match special_type {
                0 => {
                    let value = reader.read_i8().await.map_err(|e| {
                        RespError::Other(format!("Unable to parse RDB file\n{:?}", e))
                    })?;
                    Ok(value.to_string().into_bytes())
                }
                1 => {
                    let value = reader.read_i16().await.map_err(|e| {
                        RespError::Other(format!("Unable to parse RDB file\n{:?}", e))
                    })?;
                    Ok(value.to_string().into_bytes())
                }
                2 => {
                    let value = reader.read_i32().await.map_err(|e| {
                        RespError::Other(format!("Unable to parse RDB file\n{:?}", e))
                    })?;
                    Ok(value.to_string().into_bytes())
                }
                3 => unimplemented!(),
                _ => Err(RespError::Other(format!(
                    "Special encoding not supported: {:?}",
                    special_type
                ))),
            }
        }
        _ => Err(RespError::Other(format!("Unable to parse RDB file"))),
    }
}
