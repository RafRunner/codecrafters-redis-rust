use std::{collections::HashMap, net::SocketAddr, sync::Arc, time::Instant};

use anyhow::Ok;
use rand::{distributions::Alphanumeric, Rng};

use crate::{
    redis_client::RedisClient,
    redis_command::{RedisCommand, ReplConfArgs},
    redis_type::RedisType,
    server_config::ServerConfig,
};

#[derive(Debug)]
struct ValueWithExpiry {
    value: RedisType,
    expiry: Option<Instant>,
}

#[derive(Debug)]
pub struct RedisRuntime {
    values: Arc<tokio::sync::RwLock<HashMap<String, ValueWithExpiry>>>,
    config: ServerConfig,
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
                .unwrap_or_else(|| ReplicationRole::Master {
                    replicas: Vec::new(),
                }),
            replication_id: generate_alphanumeric_string(40),
            replication_offset: 0,
            config: server_config,
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
            RedisCommand::REPLCONF { .. } => todo!(),
            RedisCommand::PSYNC { .. } => {
                RedisType::simple_string(&format!("FULLRESYNC {} 0", self.replication_id))
            }
        }
    }

    pub async fn perform_handshake(&self) -> Result<(), anyhow::Error> {
        match self.replication_role {
            ReplicationRole::Master { .. } => Ok(()), // Do nothing
            ReplicationRole::Slave { replicaof } => {
                println!("Starting handshake");
                let mut client = RedisClient::new(replicaof).await?;

                println!("Sending PING");
                let response = client.send_command(&RedisCommand::PING).await?;

                response.expect_string("pong", "Unexpected return from ping")?;

                println!("Sending REPLCONF port {}", self.config.port);
                let response = client
                    .send_command(&RedisCommand::REPLCONF {
                        arg: ReplConfArgs::Port(self.config.port),
                    })
                    .await?;
                response.expect_string("ok", "Unexpected return from REPLCONF port")?;

                println!("Sending REPLCONF capabilities");
                let response: RedisType = client
                    .send_command(&RedisCommand::REPLCONF {
                        arg: ReplConfArgs::Capabilities,
                    })
                    .await?;
                response.expect_string("ok", "Unexpected return from REPLCONF capabilities")?;

                println!("Sending PSYNC");
                let response: RedisType = client
                    .send_command(&RedisCommand::PSYNC {
                        master_id: "?".to_string(),
                        master_offset: -1,
                    })
                    .await?;
                match response {
                    RedisType::SimpleString { data } => {
                        let parts: Vec<&str> = data.split_whitespace().collect();
                        if parts.len() == 3 && parts[0] == "FULLRESYNC" && parts[2] == "0" {
                            let repl_id = parts[1].to_string();
                            println!("Captured REPL_ID: {}", repl_id);
                            Ok(())
                        } else {
                            Err(anyhow::anyhow!(
                                "Unexpected format from PSYNC. Expected 'FULLRESYNC <REPL_ID> 0', received: {}",
                                data
                            ))
                        }
                    }
                    other => Err(anyhow::anyhow!(
                        "Unexpected return type from PSYNC. Expected simple string, received: {:?}",
                        other
                    )),
                }?;

                Ok(())
            }
        }
    }
}

impl Default for RedisRuntime {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

#[derive(Debug, PartialEq, Eq)]
struct Replica {
    addr: SocketAddr,
}

#[derive(Debug, PartialEq, Eq)]
enum ReplicationRole {
    Master { replicas: Vec<Replica> },
    Slave { replicaof: SocketAddr },
}

impl ReplicationRole {
    fn type_str(&self) -> &str {
        match self {
            ReplicationRole::Master { .. } => "master",
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
        assert_eq!(result, RedisType::simple_string("PONG"));
    }

    #[tokio::test]
    async fn test_echo_command() {
        let runtime = RedisRuntime::default();
        let result = runtime
            .execute(RedisCommand::ECHO("Hello, Redis!".to_string()))
            .await;
        assert_eq!(result, RedisType::bulk_string("Hello, Redis!"));
    }

    #[tokio::test]
    async fn test_set_command() {
        let runtime = RedisRuntime::default();
        let result = runtime
            .execute(RedisCommand::SET {
                key: "key1".to_string(),
                val: RedisType::bulk_string("value1"),
                ttl: None,
            })
            .await;
        assert_eq!(result, RedisType::simple_string("OK"));

        // Ensure the value is actually set
        let guard = runtime.values.read().await;
        let value = &guard.get("key1").unwrap().value;
        assert_eq!(value, &RedisType::bulk_string("value1"));
    }

    #[tokio::test]
    async fn test_set_command_with_ttl() {
        let runtime = RedisRuntime::default();

        let key = "key_with_ttl";
        let result = runtime
            .execute(RedisCommand::SET {
                key: key.to_string(),
                val: RedisType::bulk_string("temporary"),
                ttl: Some(Duration::from_millis(100)),
            })
            .await;
        assert_eq!(result, RedisType::simple_string("OK"));

        // Ensure the value is actually set
        let value = runtime
            .execute(RedisCommand::GET {
                key: key.to_string(),
            })
            .await;
        assert_eq!(value, RedisType::bulk_string("temporary"));

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
                value: RedisType::bulk_string("value1"),

                expiry: None,
            },
        );

        let result = runtime
            .execute(RedisCommand::GET {
                key: "key1".to_string(),
            })
            .await;
        assert_eq!(result, RedisType::bulk_string("value1"));
    }

    #[tokio::test]
    async fn test_replication_info() {
        let runtime = RedisRuntime::default();
        assert_eq!(
            ReplicationRole::Master {
                replicas: Vec::new()
            },
            runtime.replication_role
        );

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
            }
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
            RedisType::simple_error("Unknown arg for INFO: anything")
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
