use std::net::SocketAddr;

use tokio::{
    io::{AsyncWriteExt, BufReader},
    net::TcpStream,
};

use crate::{redis_command::RedisCommand, redis_type::RedisType, RedisWritable};

#[derive(Debug)]
pub struct RedisClient {
    addr: SocketAddr,
}

impl RedisClient {
    pub fn new(addr: SocketAddr) -> Self {
        Self { addr }
    }

    pub async fn send_command(&self, command: &RedisCommand) -> Result<RedisType, anyhow::Error> {
        let mut stream = TcpStream::connect(&self.addr).await?;

        stream.write_all(&command.write_as_protocol()).await?;

        let mut buf = BufReader::new(&mut stream);

        let response = RedisType::parse(&mut buf).await?;

        match response {
            Some(response) => Ok(response),
            None => Err(anyhow::anyhow!("Server did not respond")),
        }
    }
}
