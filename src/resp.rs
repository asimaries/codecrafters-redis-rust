use std::{array, fmt::format};

use anyhow::{Error, Ok, Result};
use bytes::BytesMut;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

#[derive(Clone, Debug)]
pub enum Value {
    SimpleString(String),
    BulkString(String),
    SimpleError(String),
    Array(Vec<Value>),
}

impl Value {
    pub fn serialize(self) -> String {
        match &self {
            Value::SimpleString(s) => format!("+{}\r\n", s),
            Value::BulkString(b) => format!("${}\r\n{}\r\n", b.chars().count(), b),
            _ => panic!("Unsupported Value for serialize"),
        }
    }
}

pub struct RespHandler {
    stream: TcpStream,
    buffer: BytesMut,
}

impl RespHandler {
    pub fn new(stream: TcpStream) -> Self {
        Self {
            stream,
            buffer: BytesMut::with_capacity(512),
        }
    }
    pub async fn read_value(&mut self) -> Result<Option<Value>> {
        let bytes_read = self.stream.read_buf(&mut self.buffer).await?;

        if bytes_read == 0 {
            return Ok(None);
        }

        let (v, _) = parse_message(self.buffer.split())?;
        Ok(Some(v))
        // Ok(Some(Value::SimpleString(String::from("+OK\r\n"))))
    }

    pub async fn write_value(&mut self, value: Value) -> Result<()> {
        self.stream.write(value.serialize().as_bytes()).await?;
        Ok(())
    }
}

pub fn parse_message(buffer: BytesMut) -> Result<(Value, usize)> {
    match buffer[0] {
        b'+' => RespParser::parse_simple_string(buffer),
        b'$' => RespParser::parse_bulk_string(buffer),
        b'*' => RespParser::parse_array(buffer),
        _ => Err(anyhow::anyhow!(format!("Unknown value type {:?}", buffer))),
    }
}

fn read_until_crlf(buffer: &[u8]) -> Option<(&[u8], usize)> {
    for i in 1..buffer.len() {
        if buffer[i - 1] == b'\r' && buffer[i] == b'\n' {
            return Some((&buffer[0..(i - 1)], i + 1));
        }
    }

    None
}
struct RespParser;
impl RespParser {
    fn parse_simple_string(buffer: BytesMut) -> Result<(Value, usize), Error> {
        if let Some((line, len)) = read_until_crlf(&buffer[1..]) {
            let string = String::from_utf8(line.to_vec()).unwrap();
            return Ok((Value::SimpleString(string), len + 1));
        }
        return Err(anyhow::anyhow!(format!("Invalid String {:?}", buffer)));
    }

    fn parse_int(buffer: &[u8]) -> Result<i64> {
        Ok(String::from_utf8(buffer.to_vec())?.parse::<i64>()?)
    }

    fn parse_array(buffer: BytesMut) -> Result<(Value, usize), Error> {
        let (array_length, mut bytes_consumed) =
            if let Some((line, len)) = read_until_crlf(&buffer[1..]) {
                let array_length = Self::parse_int(line)?;

                (array_length, len + 1)
            } else {
                return Err(anyhow::anyhow!(format!(
                    "Invalid Array format {:?}",
                    buffer
                )));
            };
        let mut items = Vec::<Value>::new();
        for _ in 0..array_length {
            let (array_item, length) = parse_message(BytesMut::from(&buffer[bytes_consumed..]))?;
            items.push(array_item);
            bytes_consumed += length;
        }

        return Ok((Value::Array(items), bytes_consumed));
    }

    fn parse_bulk_string(buffer: BytesMut) -> Result<(Value, usize)> {
        let (bulk_string_length, bytes_consumed) =
            if let Some((line, len)) = read_until_crlf(&buffer[1..]) {
                let bulk_string_length = Self::parse_int(line)?;

                (bulk_string_length, len + 1)
            } else {
                return Err(anyhow::anyhow!(format!(
                    "Invalid Array format {:?}",
                    buffer
                )));
            };

        let end_of_bulk_str = bytes_consumed + bulk_string_length as usize;
        let total_parsed = end_of_bulk_str + 2;
        return Ok((
            Value::BulkString(String::from_utf8(
                buffer[bytes_consumed..end_of_bulk_str].to_vec(),
            )?),
            total_parsed,
        ));
    }
}
