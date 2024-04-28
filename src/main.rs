use redis_starter_rust::{RedisCommand, RedisType, RedisWritable};
use tokio::io::{AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                println!("accepted new connection");

                // Spawn a new task for handling the connection
                tokio::spawn(async move {
                    match handle_connection(stream).await {
                        Ok(()) => println!("connection handled successfully"),
                        Err(e) => println!("error handling connection: {}", e),
                    }
                });
            }
            Err(e) => println!("error accepting connection: {}", e),
        }
    }
}

async fn handle_connection(mut stream: TcpStream) -> Result<(), anyhow::Error> {
    let mut buf = BufReader::new(&mut stream);

    while let Ok(Some(command)) = RedisType::parse(&mut buf).await {
        println!("Input command: {:?}", command);

        let maybe_command = RedisCommand::parse(&command)?;

        match maybe_command {
            Some(command) => {
                println!("Executing command: {:?}", command);
                let result = command.execute();
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
