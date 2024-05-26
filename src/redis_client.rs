use std::net::SocketAddr;

use tokio::{
    io::{AsyncRead, AsyncWrite, AsyncWriteExt, BufReader},
    net::TcpStream,
};

use crate::{redis_command::RedisCommand, redis_type::RedisType, RedisWritable};

#[derive(Debug)]
pub struct RedisClient<T: AsyncRead + AsyncWrite + Unpin + Send> {
    pub buffer: BufReader<T>,
}

impl<T> RedisClient<T>
where
    T: AsyncRead + AsyncWrite + Unpin + Send,
{
    pub fn new_raw(stream: T) -> Self {
        Self {
            buffer: BufReader::new(stream),
        }
    }

    pub async fn send_command(&mut self, command: &RedisCommand) -> anyhow::Result<RedisType> {
        self.buffer.write_all(&command.write_as_protocol()).await?;

        let response = RedisType::parse(&mut self.buffer).await?;
        match response {
            Some(response) => Ok(response),
            None => Err(anyhow::anyhow!("Server did not respond")),
        }
    }

    pub async fn accept_adicional_data(&mut self) -> anyhow::Result<RedisType> {
        let response = RedisType::parse(&mut self.buffer).await?;
        response.ok_or(anyhow::anyhow!(
            "Server did not provide aditional information"
        ))
    }
}

impl RedisClient<TcpStream> {
    pub async fn new(addr: SocketAddr) -> anyhow::Result<Self> {
        let stream = TcpStream::connect(&addr).await?;
        Ok(Self::new_raw(stream))
    }
}
#[cfg(test)]
mod tests {
    use crate::{rdb_file, tests::MockStream};

    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_send_command_with_mock_stream() {
        let mut write_data = Vec::new();
        let mut expected_write = Vec::new();
        let mock_stream = MockStream::new(&mut write_data);
        let mut client = RedisClient::new_raw(mock_stream);

        let command = RedisCommand::PING;
        expected_write.extend_from_slice(&command.write_as_protocol());
        let result = client.send_command(&command).await;

        // PONG simple string was recieved by the client from the stream in RESP
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), RedisType::simple_string("PONG"));

        let command = RedisCommand::ECHO("Hello mock".to_string());
        expected_write.extend_from_slice(&command.write_as_protocol());
        let result = client.send_command(&command).await;

        // Ping and ECHO commands were written to the stream in RESP protocol
        assert_eq!(write_data, expected_write);

        // Hello mock bulk string was recieved by the client from the stream in RESP
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), RedisType::bulk_string("Hello mock"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_multiple_response() {
        let mut write_data = Vec::new();
        let mock_stream = MockStream::new(&mut write_data);
        let mut client = RedisClient::new_raw(mock_stream);

        let command = RedisCommand::psync_from_scrath();
        let result = client.send_command(&command).await;

        assert!(matches!(result, Ok(RedisType::SimpleString { .. })));
        let next_result = client.accept_adicional_data().await;
        assert!(matches!(next_result, Ok(_)));
        assert_eq!(
            next_result.unwrap(),
            RedisType::RDBFile {
                file: rdb_file::get_empty_rdb_decoded()
            }
        );

        assert_eq!(write_data, command.write_as_protocol());
    }
}
