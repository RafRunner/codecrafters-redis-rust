use std::{collections::HashMap, time::Instant};

use crate::{redis_command::RedisCommand, redis_type::RedisType};

#[derive(Debug)]
struct ValueWithExpiry {
    value: RedisType,
    expiry: Option<Instant>,
}

#[derive(Debug)]
pub struct RedisRuntime {
    values: HashMap<String, ValueWithExpiry>,
}

impl RedisRuntime {
    pub fn new() -> Self {
        Self {
            values: HashMap::new(),
        }
    }

    pub fn execute(&mut self, command: RedisCommand) -> RedisType {
        match command {
            RedisCommand::PING => RedisType::SimpleString {
                data: "PONG".to_string(),
            },
            RedisCommand::ECHO(payload) => RedisType::BulkString { data: payload },
            RedisCommand::SET { key, val, ttl } => {
                self.values.insert(
                    key,
                    ValueWithExpiry {
                        value: val,
                        expiry: ttl.map(|ttl| Instant::now() + ttl),
                    },
                );

                RedisType::SimpleString {
                    data: "OK".to_string(),
                }
            }
            RedisCommand::GET { key } => {
                if let Some(val_with_expiry) = self.values.get(&key) {
                    if let Some(expiry) = val_with_expiry.expiry {
                        if Instant::now() > expiry {
                            self.values.remove(&key);
                            return RedisType::NullBulkString;
                        }
                    }
                    return val_with_expiry.value.clone();
                }

                RedisType::NullBulkString
            }
        }
    }
}

impl Default for RedisRuntime {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ping_command() {
        let mut runtime = RedisRuntime::new();
        let result = runtime.execute(RedisCommand::PING);
        assert_eq!(
            result,
            RedisType::SimpleString {
                data: "PONG".to_string()
            }
        );
    }

    #[test]
    fn test_echo_command() {
        let mut runtime = RedisRuntime::new();
        let result = runtime.execute(RedisCommand::ECHO("Hello, Redis!".to_string()));
        assert_eq!(
            result,
            RedisType::BulkString {
                data: "Hello, Redis!".to_string()
            }
        );
    }

    #[test]
    fn test_set_command() {
        let mut runtime = RedisRuntime::new();
        let result = runtime.execute(RedisCommand::SET {
            key: "key1".to_string(),
            val: RedisType::BulkString {
                data: "value1".to_string(),
            },
            ttl: None,
        });
        assert_eq!(
            result,
            RedisType::SimpleString {
                data: "OK".to_string()
            }
        );

        // Ensure the value is actually set
        let value = &runtime.values.get("key1").unwrap().value;
        assert_eq!(
            value,
            &RedisType::BulkString {
                data: "value1".to_string()
            }
        );
    }

    #[test]
    fn test_get_command_existing_key() {
        let mut runtime = RedisRuntime::new();
        runtime.values.insert(
            "key1".to_string(),
            ValueWithExpiry {
                value: RedisType::BulkString {
                    data: "value1".to_string(),
                },
                expiry: None,
            },
        );

        let result = runtime.execute(RedisCommand::GET {
            key: "key1".to_string(),
        });
        assert_eq!(
            result,
            RedisType::BulkString {
                data: "value1".to_string()
            }
        );
    }

    #[test]
    fn test_get_command_non_existing_key() {
        let mut runtime = RedisRuntime::new();

        let result = runtime.execute(RedisCommand::GET {
            key: "key1".to_string(),
        });
        assert_eq!(result, RedisType::NullBulkString);
    }
}
