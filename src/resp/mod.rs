use std::fmt::Error;

pub mod resp;
#[derive(Debug)]
pub enum RespError {
    InvalidBulkString(String),
    InvalidSimpleString(String),
    Other(String),
    Io(std::io::Error),
}

impl std::fmt::Display for RespError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RespError::Other(msg) => msg.as_str().fmt(f),
            RespError::InvalidBulkString(msg) => msg.as_str().fmt(f),
            RespError::InvalidSimpleString(msg) => msg.as_str().fmt(f),
            RespError::Io(msg) => (&msg.to_string()).as_str().fmt(f),
        }
    }
}
