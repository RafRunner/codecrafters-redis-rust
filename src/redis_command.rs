use std::time::Duration;

use crate::redis_type::RedisType;

#[derive(Debug, PartialEq, Eq)]
pub enum RedisCommand {
    PING,
    ECHO(String),
    SET {
        key: String,
        val: RedisType,
        ttl: Option<Duration>,
    },
    GET {
        key: String,
    },
    INFO {
        arg: String,
    },
}

impl RedisCommand {
    pub fn parse(data: &RedisType) -> Option<RedisCommand> {
        match data {
            RedisType::List { data } if !data.is_empty() => {
                if data.len() == 1 {
                    Self::parse(&data[0])
                } else {
                    match Self::extract_string(&data[0]) {
                        Some(command) => match command.to_lowercase().as_str() {
                            "echo" => Self::parse_echo(&data[1..]),
                            "get" => Self::parse_get(&data[1..]),
                            "set" => Self::parse_set(&data[1..]),
                            "info" => Self::parse_info(&data[1..]),
                            _ => None,
                        },
                        None => None,
                    }
                }
            }
            RedisType::BulkString { data, .. } | RedisType::SimpleString { data, .. } => {
                match data.to_lowercase().as_str() {
                    "ping" => Some(RedisCommand::PING),
                    _ => None,
                }
            }
            _ => None,
        }
    }

    fn extract_string(redis_type: &RedisType) -> Option<&str> {
        match redis_type {
            RedisType::BulkString { data, .. } | RedisType::SimpleString { data, .. } => Some(data),
            _ => None,
        }
    }

    fn parse_echo(data: &[Box<RedisType>]) -> Option<RedisCommand> {
        data.get(0).and_then(|argument| {
            Self::extract_string(argument).map(|argument| RedisCommand::ECHO(argument.to_string()))
        })
    }

    fn parse_get(data: &[Box<RedisType>]) -> Option<RedisCommand> {
        data.get(0).and_then(|key| {
            Self::extract_string(key).map(|key| RedisCommand::GET {
                key: key.to_string(),
            })
        })
    }

    fn parse_set(data: &[Box<RedisType>]) -> Option<RedisCommand> {
        if data.len() < 2 {
            return None;
        }

        let key = Self::extract_string(&data[0])?.to_string();
        let value = data[1].as_ref().clone();
        let mut ttl: Option<Duration> = None;

        // Process optional parameters
        let mut i = 2;
        while i < data.len() {
            if let Some(arg) = Self::extract_string(&data[i]) {
                match arg.to_uppercase().as_str() {
                    "PX" => {
                        ttl = data.get(i + 1).and_then(|val| {
                            Self::extract_string(val)
                                .and_then(|v| v.parse::<u64>().ok())
                                .map(Duration::from_millis)
                        });
                        i += 2; // Skip the next item since it's part of this option
                    }
                    _ => i += 1,
                }
            } else {
                i += 1;
            }
        }

        Some(RedisCommand::SET {
            key,
            val: value,
            ttl,
        })
    }

    fn parse_info(data: &[Box<RedisType>]) -> Option<RedisCommand> {
        data.get(0).and_then(|arg| {
            Self::extract_string(arg).map(|arg| RedisCommand::INFO {
                arg: arg.to_string(),
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_list(strings: &[&str]) -> RedisType {
        RedisType::List {
            data: strings
                .iter()
                .map(|elem| {
                    Box::new(RedisType::BulkString {
                        data: elem.to_string(),
                    })
                })
                .collect(),
        }
    }

    #[test]
    fn test_parse_ping() {
        let data = RedisType::SimpleString {
            data: "PING".to_string(),
        };

        let result = RedisCommand::parse(&data);
        assert_eq!(result, Some(RedisCommand::PING));

        let data = RedisType::List {
            data: vec![Box::new(RedisType::BulkString {
                data: "Ping".to_string(),
            })],
        };

        let result = RedisCommand::parse(&data);
        assert_eq!(result, Some(RedisCommand::PING));
    }

    #[test]
    fn test_parse_echo() {
        let data = RedisType::List {
            data: vec![
                Box::new(RedisType::BulkString {
                    data: "echo".to_string(),
                }),
                Box::new(RedisType::BulkString {
                    data: "hello".to_string(),
                }),
            ],
        };

        let result = RedisCommand::parse(&data);
        assert_eq!(result, Some(RedisCommand::ECHO("hello".to_string())));
    }

    #[test]
    fn test_parse_invalid() {
        let data = create_list(&["invalid", "world"]);

        let result = RedisCommand::parse(&data);
        assert_eq!(result, None);
    }

    #[test]
    fn test_parse_empty() {
        let data = RedisType::SimpleString {
            data: "".to_string(),
        };

        let result = RedisCommand::parse(&data);
        assert_eq!(result, None);
    }

    #[test]
    fn test_set_command() {
        let set = create_list(&["SET", "mykey", "myvalue"]);
        assert_eq!(
            RedisCommand::parse(&set),
            Some(RedisCommand::SET {
                key: "mykey".to_string(),
                val: RedisType::BulkString {
                    data: "myvalue".to_string(),
                },
                ttl: None
            })
        );

        let set_with_expiry = create_list(&["SET", "MyKeyTwo", "OtherValue", "px", "200"]);
        assert_eq!(
            RedisCommand::parse(&set_with_expiry),
            Some(RedisCommand::SET {
                key: "MyKeyTwo".to_string(),
                val: RedisType::BulkString {
                    data: "OtherValue".to_string(),
                },
                ttl: Some(Duration::from_millis(200))
            })
        );
    }

    #[test]
    fn test_get_command() {
        let get = create_list(&["GET", "mykey"]);
        assert_eq!(
            RedisCommand::parse(&get),
            Some(RedisCommand::GET {
                key: "mykey".to_string()
            })
        );
    }

    #[test]
    fn test_info_command() {
        let get = create_list(&["info", "replication"]);
        assert_eq!(
            RedisCommand::parse(&get),
            Some(RedisCommand::INFO {
                arg: "replication".to_string()
            })
        );
    }

    #[test]
    fn test_invalid_command() {
        let invalid = RedisType::SimpleString {
            data: "INVALID".to_string(),
        };
        assert_eq!(RedisCommand::parse(&invalid), None);
    }

    #[test]
    fn test_empty_list() {
        let empty = RedisType::List { data: vec![] };
        assert_eq!(RedisCommand::parse(&empty), None);
    }
}
