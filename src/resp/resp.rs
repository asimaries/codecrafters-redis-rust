use std::{
    array,
    fmt::{format, Error},
};

// use anyhow::};
use super::RespError;
use bytes::BytesMut;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

#[derive(Clone, Debug)]
pub enum Value {
    SimpleString(String),
    BulkString(String),
    Array(Vec<Value>),
    SimpleError(String),
    Null
}

impl Value {
    pub fn serialize(self) -> String {
        match &self {
            Value::SimpleString(s) => format!("+{}\r\n", s),
            Value::BulkString(b) => format!("${}\r\n{}\r\n", b.chars().count(), b),
            _ => "Unsupported Value for serialize".to_owned(),
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
    pub async fn read_value(&mut self) -> Result<Option<Value>, RespError> {
        let bytes_read = self
            .stream
            .read_buf(&mut self.buffer)
            .await
            .map_err(|e| RespError::Io(e))?;

        if bytes_read == 0 {
            return Ok(None);
        }

        let (v, _) = parse_message(self.buffer.split())?;
        Ok(Some(v))
        // Ok(Some(Value::SimpleString(String::from("+OK\r\n"))))
    }

    pub async fn write_value(&mut self, value: Value) -> Result<(), RespError> {
        self.stream
            .write(value.serialize().as_bytes())
            .await
            .map_err(|e| RespError::Io(e))?;
        Ok(())
    }
}

pub fn parse_message(buffer: BytesMut) -> Result<(Value, usize), RespError> {
    match buffer[0] {
        b'+' => RespParser::parse_simple_string(buffer),
        b'$' => RespParser::parse_bulk_string(buffer),
        b'*' => RespParser::parse_array(buffer),
        _ => Err(RespError::Other(
            format!("Unknown value type {:?}", buffer).to_owned(),
        )),
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
pub struct RespParser;
impl RespParser {
    fn parse_simple_string(buffer: BytesMut) -> Result<(Value, usize), RespError> {
        if let Some((line, len)) = read_until_crlf(&buffer[1..]) {
            let string = String::from_utf8(line.to_vec()).unwrap();
            return Ok((Value::SimpleString(string), len + 1));
        }
        return Err(RespError::Other(format!("Invalid String {:?}", buffer)));
    }

    fn parse_int(buffer: &[u8]) -> Result<i64, RespError> {
        let integer = String::from_utf8(buffer.to_vec())
            .map_err(|_| RespError::Other("Invalid UTF-8 sequence".to_owned()))?;

        integer
            .parse::<i64>()
            .map_err(|_| RespError::Other("Invalid Integer format".to_owned()))
    }

    fn parse_array(buffer: BytesMut) -> Result<(Value, usize), RespError> {
        let (array_length, mut bytes_consumed) =
            if let Some((line, len)) = read_until_crlf(&buffer[1..]) {
                let array_length = Self::parse_int(line)?;

                (array_length, len + 1)
            } else {
                return Err(RespError::Other(
                    format!("Invalid Array format {:?}", buffer).to_string(),
                ));
            };
        let mut items = Vec::<Value>::new();
        for _ in 0..array_length {
            let (array_item, length) = parse_message(BytesMut::from(&buffer[bytes_consumed..]))?;
            items.push(array_item);
            bytes_consumed += length;
        }

        return Ok((Value::Array(items), bytes_consumed));
    }

    pub fn parse_bulk_string(buffer: BytesMut) -> Result<(Value, usize), RespError> {
        let (bulk_string_length, bytes_consumed) =
            if let Some((line, len)) = read_until_crlf(&buffer[1..]) {
                let bulk_string_length = Self::parse_int(line)?;

                (bulk_string_length, len + 1)
            } else {
                return Err(RespError::InvalidBulkString(format!(
                    "Invalid Array format {:?}",
                    buffer
                )));
            };

        let end_of_bulk_str = bytes_consumed + bulk_string_length as usize;
        let total_parsed = end_of_bulk_str + 2;
        return Ok((
            Value::BulkString(
                String::from_utf8(buffer[bytes_consumed..end_of_bulk_str].to_vec())
                    .map_err(|e| RespError::Other("Invalid UTF-8 format".to_owned()))?,
            ),
            total_parsed,
        ));
    }
}
