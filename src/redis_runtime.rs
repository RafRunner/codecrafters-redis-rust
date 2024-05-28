use anyhow::Ok;
use base64::prelude::*;
use rand::{distributions::Alphanumeric, Rng};
use std::{
    collections::HashMap,
    net::{IpAddr, SocketAddr},
    sync::Arc,
    time::Instant,
};
use tokio::{
    io::{AsyncWriteExt, WriteHalf},
    net::TcpStream,
    sync::Mutex,
};

use crate::{
    rdb_file,
    redis_client::RedisClient,
    redis_command::{RedisCommand, ReplConfArgs},
    redis_type::RedisType,
    server_config::ServerConfig,
    RedisWritable,
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
                    replicas: Arc::new(Mutex::new(Vec::new())),
                }),
            replication_id: generate_alphanumeric_string(40),
            replication_offset: 0,
            config: server_config,
        }
    }
    pub async fn execute_no_conn(&self, command: &RedisCommand) -> RedisType {
        self.execute(command, None).await
    }

    pub async fn execute(
        &self,
        command: &RedisCommand,
        connection: Option<(IpAddr, Arc<Mutex<WriteHalf<TcpStream>>>)>,
    ) -> RedisType {
        match command {
            RedisCommand::PING => RedisType::SimpleString {
                data: "PONG".to_string(),
            },
            RedisCommand::ECHO(payload) => RedisType::BulkString {
                data: payload.clone(),
            },
            RedisCommand::SET { key, val, ttl } => {
                self.values.write().await.insert(
                    key.clone(),
                    ValueWithExpiry {
                        value: val.clone(),
                        expiry: ttl.map(|ttl| Instant::now() + ttl),
                    },
                );

                RedisType::SimpleString {
                    data: "OK".to_string(),
                }
            }
            RedisCommand::GET { key } => {
                let read_guard = self.values.read().await;

                if let Some(val_with_expiry) = read_guard.get(key) {
                    if let Some(expiry) = val_with_expiry.expiry {
                        if Instant::now() > expiry {
                            drop(read_guard);
                            self.values.write().await.remove(key);

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
            RedisCommand::REPLCONF { arg } => {
                match &arg {
                    ReplConfArgs::Port(port) => match &self.replication_role {
                        ReplicationRole::Master { replicas } => {
                            if let Some((peer_ip, connection)) = connection {
                                println!("Adding new replica at {}:{}", peer_ip, port);

                                replicas
                                    .lock()
                                    .await
                                    .push(Replica::new(connection, SocketAddr::new(peer_ip, *port)))
                            }
                        }
                        ReplicationRole::Slave { .. } => {
                            return RedisType::simple_error("You can't sync with a replica")
                        }
                    },
                    ReplConfArgs::Capabilities(_) => (),
                };
                RedisType::simple_string("OK")
            }
            RedisCommand::PSYNC {
                master_id,
                master_offset,
            } => {
                if master_id == "?" && *master_offset == -1 {
                    RedisType::multiple(vec![
                        RedisType::simple_string(&format!("FULLRESYNC {} 0", self.replication_id)),
                        RedisType::RDBFile {
                            file: rdb_file::get_empty_rdb_decoded(),
                        },
                    ])
                } else {
                    RedisType::simple_error("Not capable of syncing with those options")
                }
            }
        }
    }

    pub async fn perform_handshake(&self) -> Result<Option<TcpStream>, anyhow::Error> {
        match self.replication_role {
            ReplicationRole::Master { .. } => Ok(None), // Do nothing
            ReplicationRole::Slave { replicaof } => {
                println!("Starting handshake with {}", replicaof);
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
                let response = client
                    .send_command(&RedisCommand::default_capabilities())
                    .await?;
                response.expect_string("ok", "Unexpected return from REPLCONF capabilities")?;

                println!("Sending PSYNC");
                let response = client
                    .send_command(&RedisCommand::psync_from_scrath())
                    .await?;
                self.handle_psync(&response, &mut client).await?;

                println!("Handshake successful. Ready to receive commands");
                Ok(Some(client.buffer.into_inner()))
            }
        }
    }

    pub async fn replicate_command(&self, command: &RedisCommand) -> anyhow::Result<()> {
        if !command.is_write_command() {
            return Ok(());
        }

        if let ReplicationRole::Master { replicas } = &self.replication_role {
            for replica in replicas.lock().await.iter() {
                let mut writer = replica.connection.lock().await;
                println!("Replicating command {:?} to {}", command, replica.addr);

                if let Err(e) = writer.write_all(&command.write_as_protocol()).await {
                    println!(
                        "Error replicating command {:?} to {}. {}",
                        command, replica.addr, e
                    );
                }
            }
        }

        Ok(())
    }

    pub fn is_master(&self) -> bool {
        matches!(self.replication_role, ReplicationRole::Master { .. })
    }

    async fn handle_psync(
        &self,
        response: &RedisType,
        client: &mut RedisClient<TcpStream>,
    ) -> Result<(), anyhow::Error> {
        let repl_id = match response {
            RedisType::SimpleString { data } => self.parse_fullresync(data),
            other => {
                return Err(anyhow::anyhow!(
                    "Unexpected return type from PSYNC. Expected a simple string, received: {:?}",
                    other
                ))
            }
        }?;

        println!("Captured REPL_ID: {}", repl_id);

        let file = client.accept_rdb_file().await?;
        self.handle_rdb_file(&file)?;

        Ok(())
    }

    fn parse_fullresync(&self, data: &str) -> Result<String, anyhow::Error> {
        let parts: Vec<&str> = data.split_whitespace().collect();
        if parts.len() == 3 && parts[0] == "FULLRESYNC" && parts[2] == "0" {
            let repl_id = parts[1].to_string();
            Ok(repl_id)
        } else {
            Err(anyhow::anyhow!(
                "Unexpected format from PSYNC. Expected 'FULLRESYNC <REPL_ID> 0', received: {}",
                data
            ))
        }
    }

    fn handle_rdb_file(&self, response: &RedisType) -> Result<(), anyhow::Error> {
        if let RedisType::RDBFile { file } = response {
            let file_text = BASE64_STANDARD.encode(file);
            println!("Received file: {}", file_text);

            if String::from_utf8_lossy(&file[..5]) == "REDIS" {
                Ok(())
            } else {
                Err(anyhow::anyhow!("File is not an RDB file!"))
            }
        } else {
            Err(anyhow::anyhow!(
                "Unexpected type for RDB file. Expected a RDB file, received: {:?}",
                response
            ))
        }
    }
}

impl Default for RedisRuntime {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

#[derive(Debug)]
struct Replica {
    connection: Arc<Mutex<WriteHalf<TcpStream>>>,
    addr: SocketAddr,
}

impl Replica {
    fn new(client: Arc<Mutex<WriteHalf<TcpStream>>>, addr: SocketAddr) -> Self {
        Self {
            connection: client,
            addr,
        }
    }
}

#[derive(Debug)]
enum ReplicationRole {
    Master { replicas: Arc<Mutex<Vec<Replica>>> },
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
        let result = runtime.execute_no_conn(&RedisCommand::PING).await;
        assert_eq!(result, RedisType::simple_string("PONG"));
    }

    #[tokio::test]
    async fn test_echo_command() {
        let runtime = RedisRuntime::default();
        let result = runtime
            .execute_no_conn(&RedisCommand::ECHO("Hello, Redis!".to_string()))
            .await;
        assert_eq!(result, RedisType::bulk_string("Hello, Redis!"));
    }

    #[tokio::test]
    async fn test_set_command() {
        let runtime = RedisRuntime::default();
        let result = runtime
            .execute_no_conn(&RedisCommand::SET {
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
            .execute_no_conn(&RedisCommand::SET {
                key: key.to_string(),
                val: RedisType::bulk_string("temporary"),
                ttl: Some(Duration::from_millis(100)),
            })
            .await;
        assert_eq!(result, RedisType::simple_string("OK"));

        // Ensure the value is actually set
        let value = runtime
            .execute_no_conn(&RedisCommand::GET {
                key: key.to_string(),
            })
            .await;
        assert_eq!(value, RedisType::bulk_string("temporary"));

        tokio::time::sleep(Duration::from_millis(101)).await;

        // Ensure the value has expired
        let value = runtime
            .execute_no_conn(&RedisCommand::GET {
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
            .execute_no_conn(&RedisCommand::GET {
                key: "key1".to_string(),
            })
            .await;
        assert_eq!(result, RedisType::bulk_string("value1"));
    }

    #[tokio::test]
    async fn test_replication_info() {
        let runtime = RedisRuntime::default();
        assert!(matches!(
            runtime.replication_role,
            ReplicationRole::Master { .. },
        ));

        let result = runtime
            .execute_no_conn(&RedisCommand::INFO {
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
            .execute_no_conn(&RedisCommand::INFO {
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
            .execute_no_conn(&RedisCommand::GET {
                key: "key1".to_string(),
            })
            .await;
        assert_eq!(result, RedisType::NullBulkString);
    }

    #[tokio::test]
    async fn test_replconf() {
        let runtime = RedisRuntime::default();

        let result = runtime
            .execute_no_conn(&RedisCommand::REPLCONF {
                arg: ReplConfArgs::Port(1234),
            })
            .await;
        assert_eq!(result, RedisType::simple_string("OK"));

        let result = runtime
            .execute_no_conn(&RedisCommand::REPLCONF {
                arg: ReplConfArgs::Capabilities(vec!["psync2".to_string()]),
            })
            .await;
        assert_eq!(result, RedisType::simple_string("OK"));
    }
}
