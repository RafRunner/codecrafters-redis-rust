use crate::redis_type::RedisType;

#[derive(Debug, PartialEq, Eq)]
pub enum RedisCommand {
    PING,
    ECHO(String),
    SET { key: String, val: RedisType },
    GET { key: String },
}

impl RedisCommand {
    pub fn parse(data: &RedisType) -> Option<RedisCommand> {
        match data {
            RedisType::List { data } if !data.is_empty() => match data.len() {
                1 => Self::parse(&data[0]),
                2 => Self::parse_two_element_command(data),
                3 => Self::parse_three_element_command(data),
                _ => None,
            },
            RedisType::BulkString { data, .. } | RedisType::SimpleString { data, .. } => {
                match data.to_lowercase().as_str() {
                    "ping" => Some(RedisCommand::PING),
                    _ => None,
                }
            }
            _ => None,
        }
    }

    fn parse_two_element_command(data: &[Box<RedisType>]) -> Option<RedisCommand> {
        let command = Self::extract_string(&data[0])?.to_lowercase();
        let argument = Self::extract_string(&data[1])?;

        match command.as_str() {
            "echo" => Some(RedisCommand::ECHO(argument.to_string())),
            "get" => Some(RedisCommand::GET {
                key: argument.to_string(),
            }),
            _ => None,
        }
    }

    fn parse_three_element_command(data: &[Box<RedisType>]) -> Option<RedisCommand> {
        let command = Self::extract_string(&data[0])?.to_lowercase();
        match command.as_str() {
            "set" => {
                let key = Self::extract_string(&data[1])?.to_string();
                let val = data[2].as_ref().clone();
                Some(RedisCommand::SET { key, val })
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
        let data = RedisType::List {
            data: vec![
                Box::new(RedisType::BulkString {
                    data: "invalid".to_string(),
                }),
                Box::new(RedisType::BulkString {
                    data: "world".to_string(),
                }),
            ],
        };

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
        let set = RedisType::List {
            data: vec![
                Box::new(RedisType::SimpleString {
                    data: "SET".to_string(),
                }),
                Box::new(RedisType::SimpleString {
                    data: "mykey".to_string(),
                }),
                Box::new(RedisType::BulkString {
                    data: "myvalue".to_string(),
                }),
            ],
        };
        assert_eq!(
            RedisCommand::parse(&set),
            Some(RedisCommand::SET {
                key: "mykey".to_string(),
                val: RedisType::BulkString {
                    data: "myvalue".to_string()
                }
            })
        );
    }

    #[test]
    fn test_get_command() {
        let get = RedisType::List {
            data: vec![
                Box::new(RedisType::SimpleString {
                    data: "GET".to_string(),
                }),
                Box::new(RedisType::SimpleString {
                    data: "mykey".to_string(),
                }),
            ],
        };
        assert_eq!(
            RedisCommand::parse(&get),
            Some(RedisCommand::GET {
                key: "mykey".to_string()
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
