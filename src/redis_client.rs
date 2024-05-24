use std::net::SocketAddr;

use tokio::{
    io::{AsyncRead, AsyncWrite, AsyncWriteExt, BufReader},
    net::TcpStream,
};

use crate::{redis_command::RedisCommand, redis_type::RedisType, RedisWritable};

#[derive(Debug)]
pub struct RedisClient<T: AsyncRead + AsyncWrite + Unpin + Send> {
    stream: T,
}

impl<T> RedisClient<T>
where
    T: AsyncRead + AsyncWrite + Unpin + Send,
{
    pub fn new_raw(stream: T) -> Self {
        Self { stream }
    }

    pub async fn send_command(
        &mut self,
        command: &RedisCommand,
    ) -> Result<RedisType, anyhow::Error> {
        self.stream.write_all(&command.write_as_protocol()).await?;

        let mut buf = BufReader::new(&mut self.stream);

        let response = RedisType::parse(&mut buf).await?;
        match response {
            Some(response) => {
                // We loop to figure if the server responded with more than one value
                let mut multiple_res = vec![response];
                while let Some(more_data) = RedisType::parse(&mut buf).await? {
                    multiple_res.push(more_data);
                }

                Ok(match multiple_res.len() {
                    1 => multiple_res.remove(0),
                    _ => RedisType::multiple(multiple_res),
                })
            }
            None => Err(anyhow::anyhow!("Server did not respond")),
        }
    }
}

impl RedisClient<TcpStream> {
    pub async fn new(addr: SocketAddr) -> Result<Self, anyhow::Error> {
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

        let command = RedisCommand::PSYNC {
            master_id: "?".to_string(),
            master_offset: 0,
        };
        let result = client.send_command(&command).await;

        assert_eq!(write_data, command.write_as_protocol());

        assert!(result.is_ok());
        match result.unwrap() {
            RedisType::MultipleType { values } => {
                let mut iter = values.into_iter();
                assert!(matches!(
                    *iter.next().unwrap(),
                    RedisType::SimpleString { .. }
                ));
                assert_eq!(
                    *iter.next().unwrap(),
                    RedisType::RDBFile {
                        file: rdb_file::get_empty_rdb_decoded()
                    }
                )
            }
            other => panic!(
                "Server should have responded with multiple values. Reponse: {:?}",
                other
            ),
        }
    }
}
