use std::collections::HashMap;

use crate::{redis_command::RedisCommand, redis_type::RedisType};

#[derive(Debug)]
pub struct RedisRuntime {
    values: HashMap<String, RedisType>,
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
            RedisCommand::SET { key, val } => {
                self.values.insert(key, val);

                RedisType::SimpleString {
                    data: "OK".to_string(),
                }
            }
            RedisCommand::GET { key } => match self.values.get(&key) {
                Some(val) => val.clone(),
                None => RedisType::NullBulkString,
            },
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
        });
        assert_eq!(
            result,
            RedisType::SimpleString {
                data: "OK".to_string()
            }
        );

        // Ensure the value is actually set
        let value = runtime.values.get("key1").unwrap();
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
            RedisType::BulkString {
                data: "value1".to_string(),
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
