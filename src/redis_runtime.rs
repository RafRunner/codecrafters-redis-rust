use std::{collections::HashMap, net::SocketAddr, sync::Arc, time::Instant};

use rand::{distributions::Alphanumeric, Rng};

use crate::{redis_command::RedisCommand, redis_type::RedisType, server_config::ServerConfig};

#[derive(Debug)]
struct ValueWithExpiry {
    value: RedisType,
    expiry: Option<Instant>,
}

#[derive(Debug)]
pub struct RedisRuntime {
    values: Arc<tokio::sync::RwLock<HashMap<String, ValueWithExpiry>>>,
    replication_role: ReplicationRole,
    replication_id: String,
    replication_offset: u16,
}

impl RedisRuntime {
    pub fn new(server_config: ServerConfig) -> Self {
        Self {
            values: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            replication_role: server_config
                .replica_addr
                .map(|addr| ReplicationRole::Slave { replicaof: addr })
                .unwrap_or(ReplicationRole::Master),
            replication_id: generate_alphanumeric_string(40),
            replication_offset: 0,
        }
    }

    pub async fn execute(&self, command: RedisCommand) -> RedisType {
        match command {
            RedisCommand::PING => RedisType::SimpleString {
                data: "PONG".to_string(),
            },
            RedisCommand::ECHO(payload) => RedisType::BulkString { data: payload },
            RedisCommand::SET { key, val, ttl } => {
                self.values.write().await.insert(
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
                let read_guard = self.values.read().await;

                if let Some(val_with_expiry) = read_guard.get(&key) {
                    if let Some(expiry) = val_with_expiry.expiry {
                        if Instant::now() > expiry {
                            drop(read_guard);
                            self.values.write().await.remove(&key);

                            return RedisType::NullBulkString;
                        }
                    }
                    return val_with_expiry.value.clone();
                }

                RedisType::NullBulkString
            }
            RedisCommand::INFO { arg } => match arg.to_lowercase().as_str() {
                "replication" => RedisType::BulkString {
                    data: format!(
                        "role:{}
master_replid:{}
master_repl_offset:{}",
                        self.replication_role.type_str(),
                        self.replication_id,
                        self.replication_offset
                    ),
                },
                unknown => RedisType::SimpleError {
                    message: format!("Unknown arg for INFO: {}", unknown),
                },
            },
        }
    }
}

impl Default for RedisRuntime {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

#[derive(Debug, PartialEq, Eq)]
enum ReplicationRole {
    Master,
    Slave { replicaof: SocketAddr },
}

impl ReplicationRole {
    fn type_str(&self) -> &str {
        match self {
            ReplicationRole::Master => "master",
            ReplicationRole::Slave { .. } => "slave",
        }
    }
}

fn generate_alphanumeric_string(length: usize) -> String {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(length)
        .map(char::from)
        .collect()
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[tokio::test]
    async fn test_ping_command() {
        let runtime = RedisRuntime::default();
        let result = runtime.execute(RedisCommand::PING).await;
        assert_eq!(
            result,
            RedisType::SimpleString {
                data: "PONG".to_string()
            }
        );
    }

    #[tokio::test]
    async fn test_echo_command() {
        let runtime = RedisRuntime::default();
        let result = runtime
            .execute(RedisCommand::ECHO("Hello, Redis!".to_string()))
            .await;
        assert_eq!(
            result,
            RedisType::BulkString {
                data: "Hello, Redis!".to_string()
            }
        );
    }

    #[tokio::test]
    async fn test_set_command() {
        let runtime = RedisRuntime::default();
        let result = runtime
            .execute(RedisCommand::SET {
                key: "key1".to_string(),
                val: RedisType::BulkString {
                    data: "value1".to_string(),
                },
                ttl: None,
            })
            .await;
        assert_eq!(
            result,
            RedisType::SimpleString {
                data: "OK".to_string()
            }
        );

        // Ensure the value is actually set
        let guard = runtime.values.read().await;
        let value = &guard.get("key1").unwrap().value;
        assert_eq!(
            value,
            &RedisType::BulkString {
                data: "value1".to_string()
            }
        );
    }

    #[tokio::test]
    async fn test_set_command_with_ttl() {
        let runtime = RedisRuntime::default();

        let key = "key_with_ttl";
        let result = runtime
            .execute(RedisCommand::SET {
                key: key.to_string(),
                val: RedisType::BulkString {
                    data: "temporary".to_string(),
                },
                ttl: Some(Duration::from_millis(100)),
            })
            .await;
        assert_eq!(
            result,
            RedisType::SimpleString {
                data: "OK".to_string()
            }
        );

        // Ensure the value is actually set
        let value = runtime
            .execute(RedisCommand::GET {
                key: key.to_string(),
            })
            .await;
        assert_eq!(
            value,
            RedisType::BulkString {
                data: "temporary".to_string()
            }
        );

        tokio::time::sleep(Duration::from_millis(101)).await;

        // Ensure the value has expired
        let value = runtime
            .execute(RedisCommand::GET {
                key: key.to_string(),
            })
            .await;
        assert_eq!(value, RedisType::NullBulkString);
    }

    #[tokio::test]
    async fn test_get_command_existing_key() {
        let runtime = RedisRuntime::default();
        runtime.values.write().await.insert(
            "key1".to_string(),
            ValueWithExpiry {
                value: RedisType::BulkString {
                    data: "value1".to_string(),
                },
                expiry: None,
            },
        );

        let result = runtime
            .execute(RedisCommand::GET {
                key: "key1".to_string(),
            })
            .await;
        assert_eq!(
            result,
            RedisType::BulkString {
                data: "value1".to_string()
            }
        );
    }

    #[tokio::test]
    async fn test_replication_info() {
        let runtime = RedisRuntime::default();
        assert_eq!(ReplicationRole::Master, runtime.replication_role);

        let result = runtime
            .execute(RedisCommand::INFO {
                arg: "replication".to_string(),
            })
            .await;

            match result {
                RedisType::BulkString { data } => {
                    assert!(data.contains("role:master"));
                    assert!(data.contains("master_replid:"));
                    assert!(data.contains("master_repl_offset:0"));
                },
                _ => panic!("Result was not a bulk string"),
            }
    }

    #[tokio::test]
    async fn test_unknown_info() {
        let runtime = RedisRuntime::default();

        let result = runtime
            .execute(RedisCommand::INFO {
                arg: "anything".to_string(),
            })
            .await;
        assert_eq!(
            result,
            RedisType::SimpleError {
                message: "Unknown arg for INFO: anything".to_string()
            }
        );
    }

    #[tokio::test]
    async fn test_get_command_non_existing_key() {
        let runtime = RedisRuntime::default();

        let result = runtime
            .execute(RedisCommand::GET {
                key: "key1".to_string(),
            })
            .await;
        assert_eq!(result, RedisType::NullBulkString);
    }
}
