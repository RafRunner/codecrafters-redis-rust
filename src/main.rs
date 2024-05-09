use std::sync::Arc;

use redis_starter_rust::redis_runtime::RedisRuntime;
use redis_starter_rust::{redis_command::RedisCommand, redis_type::RedisType, RedisWritable};
use tokio::io::{AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    let runtime = Arc::new(RedisRuntime::new());

    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                println!("accepted new connection");
                let runtime_clone = Arc::clone(&runtime);

                // Spawn a new task for handling the connection
                tokio::spawn(async move {
                    match handle_connection(stream, runtime_clone.as_ref()).await {
                        Ok(()) => println!("connection handled successfully"),
                        Err(e) => println!("error handling connection: {}", e),
                    }
                });
            }
            Err(e) => println!("error accepting connection: {}", e),
        }
    }
}

async fn handle_connection(
    mut stream: TcpStream,
    runtine: &RedisRuntime,
) -> Result<(), anyhow::Error> {
    let mut buf = BufReader::new(&mut stream);

    while let Ok(Some(input)) = RedisType::parse(&mut buf).await {
        println!("Input type: {:?}", input);

        match RedisCommand::parse(&input) {
            Some(command) => {
                println!("Executing command: {:?}", command);
                let result = runtine.execute(command).await;
                println!("Command result: {:?}", result);

                buf.write_all(&result.write_as_protocol()).await?
            }
            None => {
                println!("No response built");
                buf.write_all(
                    &RedisType::SimpleError {
                        message: "Unrecognized command".to_string(),
                    }
                    .write_as_protocol(),
                )
                .await?
            }
        }
    }

    Ok(())
}
