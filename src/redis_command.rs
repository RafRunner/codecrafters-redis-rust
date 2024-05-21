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
    REPLCONF {
        arg: ReplConfArgs,
    },
    PSYNC {
        master_id: String,
        master_offset: i64,
    },
}

impl RedisCommand {
    pub fn parse(data: &RedisType) -> Option<RedisCommand> {
        match data {
            RedisType::List { data } if !data.is_empty() => {
                if data.len() == 1 {
                    Self::parse(&data[0])
                } else {
                    let rest = &data[1..];

                    match data[0].extract_string() {
                        Some(command) => match command.to_lowercase().as_str() {
                            "echo" => Self::parse_echo(rest),
                            "get" => Self::parse_get(rest),
                            "set" => Self::parse_set(rest),
                            "info" => Self::parse_info(rest),
                            "replconf" => Self::parse_replconf(rest),
                            "psync" => Self::parse_psync(rest),
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

    fn parse_echo(data: &[Box<RedisType>]) -> Option<RedisCommand> {
        data.get(0)
            .and_then(|argument| argument.extract_string())
            .map(|argument| RedisCommand::ECHO(argument.to_string()))
    }

    fn parse_get(data: &[Box<RedisType>]) -> Option<RedisCommand> {
        data.get(0)
            .and_then(|key| key.extract_string())
            .map(|key| RedisCommand::GET {
                key: key.to_string(),
            })
    }

    fn parse_set(data: &[Box<RedisType>]) -> Option<RedisCommand> {
        if data.len() < 2 {
            return None;
        }

        let key = data[0].extract_string()?.to_string();
        let value = data[1].as_ref().clone();
        let mut ttl: Option<Duration> = None;

        // Process optional parameters
        let mut i = 2;
        while i < data.len() {
            if let Some(arg) = data[i].extract_string() {
                match arg.to_uppercase().as_str() {
                    "PX" => {
                        ttl = data.get(i + 1).and_then(|val| {
                            val.extract_string()
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
        data.get(0)
            .and_then(|arg| arg.extract_string())
            .map(|arg| RedisCommand::INFO {
                arg: arg.to_string(),
            })
    }

    fn parse_replconf(data: &[Box<RedisType>]) -> Option<RedisCommand> {
        if data.len() < 2 {
            return None;
        }

        match data[0].extract_string() {
            Some("listening-port") => {
                let port: u16 = data[1].extract_string().and_then(|raw| raw.parse().ok())?;

                Some(RedisCommand::REPLCONF {
                    arg: ReplConfArgs::Port(port),
                })
            }
            Some("capa") => match data[1].extract_string() {
                Some("psync2") => Some(RedisCommand::REPLCONF {
                    arg: ReplConfArgs::Capabilities,
                }),
                _ => None,
            },
            _ => None,
        }
    }

    fn parse_psync(data: &[Box<RedisType>]) -> Option<RedisCommand> {
        if data.len() != 2 {
            return None;
        }

        let master_id = data[0].extract_string()?.to_string();
        let master_offset = data[1].extract_string()?.parse().ok()?;

        Some(RedisCommand::PSYNC {
            master_id,
            master_offset,
        })
    }
}

impl RedisWritable for RedisCommand {
    fn write_as_protocol(&self) -> Vec<u8> {
        let parts = match self {
            Self::PING => vec![RedisType::bulk_string("PING")],
            Self::ECHO(value) => vec![
                RedisType::bulk_string("ECHO"),
                RedisType::bulk_string(value),
            ],
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

                command
            }
            Self::GET { key } => vec![RedisType::bulk_string("GET"), RedisType::bulk_string(key)],
            Self::INFO { arg } => vec![RedisType::bulk_string("INFO"), RedisType::bulk_string(arg)],
            Self::REPLCONF { arg } => {
                let mut command = vec![RedisType::bulk_string("REPLCONF")];

                match arg {
                    ReplConfArgs::Port(port) => {
                        command.push(RedisType::bulk_string("listening-port"));
                        command.push(RedisType::bulk_string(&port.to_string()))
                    }
                    ReplConfArgs::Capabilities => {
                        command.push(RedisType::bulk_string("capa"));
                        command.push(RedisType::bulk_string("psync2"))
                    }
                };

                command
            }
            Self::PSYNC {
                master_id,
                master_offset,
            } => vec![
                RedisType::bulk_string("PSYNC"),
                RedisType::bulk_string(master_id),
                RedisType::bulk_string(&master_offset.to_string()),
            ],
        };

        RedisType::list(parts).write_as_protocol()
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum ReplConfArgs {
    Port(u16),
    Capabilities,
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

    #[test]
    fn test_parse_replconf() {
        let data = RedisType::list(vec![
            RedisType::bulk_string("replconf"),
            RedisType::bulk_string("listening-port"),
            RedisType::bulk_string("6379"),
        ]);

        let result = RedisCommand::parse(&data);
        assert_eq!(
            result,
            Some(RedisCommand::REPLCONF {
                arg: ReplConfArgs::Port(6379)
            })
        );

        let data = RedisType::list(vec![
            RedisType::bulk_string("replconf"),
            RedisType::bulk_string("capa"),
            RedisType::bulk_string("psync2"),
        ]);

        let result = RedisCommand::parse(&data);
        assert_eq!(
            result,
            Some(RedisCommand::REPLCONF {
                arg: ReplConfArgs::Capabilities
            })
        );
    }

    #[test]
    fn test_parse_invalid_replconf() {
        let data = RedisType::list(vec![
            RedisType::bulk_string("replconf"),
            RedisType::bulk_string("unknown"),
        ]);

        let result = RedisCommand::parse(&data);
        assert_eq!(result, None);

        let data = RedisType::list(vec![
            RedisType::bulk_string("replconf"),
            RedisType::bulk_string("listening-port"),
            RedisType::bulk_string("invalid-port"),
        ]);

        let result = RedisCommand::parse(&data);
        assert_eq!(result, None);
    }

    #[test]
    fn test_parse_psync() {
        let data = RedisType::list(vec![
            RedisType::bulk_string("PSYNC"),
            RedisType::bulk_string("?"),
            RedisType::bulk_string("-1"),
        ]);

        let result = RedisCommand::parse(&data);
        assert_eq!(
            result,
            Some(RedisCommand::PSYNC {
                master_id: "?".to_string(),
                master_offset: -1
            })
        );

        let data = RedisType::list(vec![
            RedisType::bulk_string("PSYNC"),
            RedisType::bulk_string("4324fdsgbfdh235"),
        ]);

        let result = RedisCommand::parse(&data);
        assert_eq!(result, None);
    }
}
