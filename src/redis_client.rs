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
            Some(response) => Ok(response),
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
    use std::{
        pin::Pin,
        sync::{Arc, Mutex},
        task::{Context, Poll},
    };

    use crate::{redis_runtime::RedisRuntime, server_config::ServerConfig};

    use super::*;
    use tokio::{
        io::{self, AsyncRead, AsyncWrite},
        runtime::Handle,
    };

    struct MockStream<'a> {
        read_data: Vec<u8>,
        write_data: &'a mut Vec<u8>,
        runtime: Arc<Mutex<RedisRuntime>>,
    }

    impl<'a> MockStream<'a> {
        pub fn new(write_data: &'a mut Vec<u8>) -> Self {
            Self {
                read_data: Vec::new(),
                write_data,
                runtime: Arc::new(Mutex::new(RedisRuntime::new(ServerConfig::default()))),
            }
        }
    }

    impl<'a> AsyncRead for MockStream<'a> {
        fn poll_read(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
            buf: &mut tokio::io::ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            let this = self.get_mut();
            buf.put_slice(&this.read_data);
            this.read_data = Vec::new();

            Poll::Ready(Ok(()))
        }
    }

    impl<'a> AsyncWrite for MockStream<'a> {
        fn poll_write(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            let this = self.get_mut();
            this.write_data.extend_from_slice(buf);

            tokio::task::block_in_place(move || {
                Handle::current().block_on(async {
                    let argument = RedisType::parse(&mut BufReader::new(buf))
                        .await
                        .unwrap()
                        .unwrap();

                    // Simulate server processing the command
                    let command = RedisCommand::parse(&argument).unwrap();
                    let response = this.runtime.lock().unwrap().execute(command).await;

                    // Prepare response to be read by the client
                    this.read_data.extend_from_slice(&response.write_as_protocol());
                })
            });

            Poll::Ready(Ok(buf.len()))
        }

        fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

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
}
