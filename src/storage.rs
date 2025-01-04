use std::{collections::HashMap, sync::Arc, time::Instant};

use tokio::sync::RwLock;

use crate::resp::resp::Value;

pub(crate) struct Item {
    pub value: Value,
    pub created_at: Instant,
    pub ttl: Option<usize>,
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

    pub async fn set(&mut self, key: String, value: String, ttl: Option<usize>) -> Value {
        println!("key {}\nvalue {}\nexpies {:?}", key, value, ttl);
        self.storage.insert(
            key,
            Item {
                value: Value::BulkString(value),
                created_at: Instant::now(),
                ttl,
            },
        );
        Value::SimpleString("OK".to_owned())
    }

    pub async fn get(&self, key: String) -> Value {
        match self.storage.get(&key) {
            Some(item) => {
                let is_expired = item.ttl.map_or(false, |ttl| {
                    ttl > 0 && item.created_at.elapsed().as_millis() > ttl as u128
                });
                match is_expired {
                    true => Value::Null,
                    false => item.value.clone(),
                }
            }
            None => Value::Null,
        }
    }
}
