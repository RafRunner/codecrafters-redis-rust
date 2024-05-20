use std::time::Duration;

use crate::{redis_type::RedisType, RedisWritable};

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

impl RedisWritable for RedisCommand {
    fn write_as_protocol(&self) -> Vec<u8> {
        match self {
            Self::PING => RedisType::bulk_string("PONG").write_as_protocol(),
            Self::ECHO(value) => RedisType::bulk_string(value).write_as_protocol(),
            Self::SET { key, val, ttl } => {
                let mut command = vec![
                    RedisType::bulk_string("SET"),
                    RedisType::bulk_string(key),
                    val.clone(),
                ];

                if let Some(ttl) = ttl {
                    command.push(RedisType::bulk_string("px"));
                    command.push(RedisType::bulk_string(&ttl.as_millis().to_string()));
                }

                RedisType::list(command).write_as_protocol()
            }
            Self::GET { key } => {
                let command = vec![RedisType::bulk_string("GET"), RedisType::bulk_string(key)];

                RedisType::list(command).write_as_protocol()
            }
            Self::INFO { arg } => {
                let command = vec![RedisType::bulk_string("info"), RedisType::bulk_string(arg)];

                RedisType::list(command).write_as_protocol()
            }
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_ping() {
        let data = RedisType::SimpleString {
            data: "PING".to_string(),
        };

        let result = RedisCommand::parse(&data);
        assert_eq!(result, Some(RedisCommand::PING));

        let data = RedisType::list(vec![RedisType::bulk_string("Ping")]);

        let result = RedisCommand::parse(&data);
        assert_eq!(result, Some(RedisCommand::PING));
    }

    #[test]
    fn test_parse_echo() {
        let data = RedisType::list(vec![
            RedisType::bulk_string("echo"),
            RedisType::bulk_string("hello"),
        ]);

        let result = RedisCommand::parse(&data);
        assert_eq!(result, Some(RedisCommand::ECHO("hello".to_string())));
    }

    #[test]
    fn test_parse_invalid() {
        let data = RedisType::list(vec![
            RedisType::bulk_string("invalid"),
            RedisType::bulk_string("world"),
        ]);

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
        let set = RedisType::list(vec![
            RedisType::bulk_string("SET"),
            RedisType::bulk_string("mykey"),
            RedisType::bulk_string("myvalue"),
        ]);
        assert_eq!(
            RedisCommand::parse(&set),
            Some(RedisCommand::SET {
                key: "mykey".to_string(),
                val: RedisType::bulk_string("myvalue"),
                ttl: None
            })
        );

        let set_with_expiry = RedisType::list(vec![
            RedisType::bulk_string("SET"),
            RedisType::bulk_string("MyKeyTwo"),
            RedisType::bulk_string("OtherValue"),
            RedisType::bulk_string("px"),
            RedisType::bulk_string("200"),
        ]);
        assert_eq!(
            RedisCommand::parse(&set_with_expiry),
            Some(RedisCommand::SET {
                key: "MyKeyTwo".to_string(),
                val: RedisType::bulk_string("OtherValue"),

                ttl: Some(Duration::from_millis(200))
            })
        );
    }

    #[test]
    fn test_get_command() {
        let get = RedisType::list(vec![
            RedisType::bulk_string("GET"),
            RedisType::bulk_string("mykey"),
        ]);
        assert_eq!(
            RedisCommand::parse(&get),
            Some(RedisCommand::GET {
                key: "mykey".to_string()
            })
        );
    }

    #[test]
    fn test_info_command() {
        let get = RedisType::list(vec![
            RedisType::bulk_string("info"),
            RedisType::bulk_string("replication"),
        ]);
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
        let empty = RedisType::list(vec![]);
        assert_eq!(RedisCommand::parse(&empty), None);
    }
}
