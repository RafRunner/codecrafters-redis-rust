use std::net::SocketAddr;

use tokio::{
    io::{AsyncWriteExt, BufReader},
    net::TcpStream,
};

use crate::{redis_command::RedisCommand, redis_type::RedisType, RedisWritable};

#[derive(Debug)]
pub struct RedisClient {
    stream: TcpStream,
}

impl RedisClient {
    pub async fn new(addr: SocketAddr) -> Result<Self, anyhow::Error> {
        let stream = TcpStream::connect(&addr).await?;
        Ok(Self { stream })
    }

    pub async fn send_command(
        &mut self,
        command: &RedisCommand,
    ) -> Result<RedisType, anyhow::Error> {
        self.stream.write_all(&command.write_as_protocol()).await?;

        let mut buf = BufReader::new(&mut self.stream);

        let response = RedisType::parse(&mut buf).await?;
        match response {
            Some(response) => Ok(response),
            None => Err(anyhow::anyhow!("Server did not respond")),
        }
    }
}
