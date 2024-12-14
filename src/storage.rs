use std::{collections::HashMap, time::Instant};

use crate::resp::resp::Value;

struct Item {
    pub value: Value,
    pub created_at: Instant,
    pub expires: usize,
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
    pub fn set(&mut self, key: String, value: String, expires: usize) -> Value {
        println!("key {}\nvalue {}\nexpies {}", key, value, expires);
        self.storage.insert(
            key,
            Item {
                value: Value::BulkString(value),
                created_at: Instant::now(),
                expires,
            },
        );
        Value::SimpleString("OK".to_owned())
    }
    pub fn get(&self, key: String) -> Value {
        match self.storage.get(&key) {
            Some(item) => {
                let is_expired = item.expires > 0
                    && (item.created_at.elapsed().as_millis() > item.expires as u128);
                println!(
                    "{} {} {}",
                    item.created_at.elapsed().as_millis(),
                    item.expires as u128,
                    Instant::now().elapsed().as_millis()
                );
                match is_expired {
                    true => Value::Null,
                    false => item.value.clone(),
                }
            }
            None => Value::Null,
        }
    }
}
