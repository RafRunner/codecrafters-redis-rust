pub mod rdb_file;
pub mod redis_client;
pub mod redis_command;
pub mod redis_runtime;
pub mod redis_type;
pub mod server_config;

pub trait RedisWritable {
    fn write_as_protocol(&self) -> Vec<u8>;
}

#[cfg(test)]
pub mod tests {
    use std::{
        pin::Pin,
        sync::{Arc, Mutex},
        task::{Context, Poll},
    };

    use crate::{
        redis_command::RedisCommand, redis_runtime::RedisRuntime, redis_type::RedisType,
        server_config::ServerConfig, RedisWritable,
    };
    use tokio::{
        io::{self, AsyncRead, AsyncWrite, BufReader},
        runtime::Handle,
    };

    pub struct MockStream<'a> {
        pub read_data: Vec<u8>,
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
                    let response = this.runtime.lock().unwrap().execute_no_conn(&command).await;

                    // Prepare response to be read by the client
                    this.read_data
                        .extend_from_slice(&response.write_as_protocol());
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
}
