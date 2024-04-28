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

    let command = RedisType::parse(&mut buf).await?;

    println!("Input command: {:?}", command);

    let response = match command {
        RedisType::List { len, data } => {
            let vector = data;

            if len == 2 {
                if let RedisType::BulkString { data, .. } | RedisType::SimpleString { data, .. } =
                    vector[0].as_ref()
                {
                    if data.to_lowercase() == "ECHO" {
                        if let RedisType::BulkString { data, .. }
                        | RedisType::SimpleString { data, .. } = vector[1].as_ref()
                        {
                            Some(RedisCommand::ECHO(data.clone()))
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                } else {
                    None
                }
            } else {
                None
            }
        }
        RedisType::BulkString { data, .. } | RedisType::SimpleString { data, .. } => {
            if data.to_lowercase() == "ping" {
                Some(RedisCommand::PING)
            } else {
                None
            }
        }
        RedisType::SimpleError { .. } => None,
    };

    match response {
        Some(command) => {
            println!("Writing response: {:?}", command);
            buf.write_all(&command.write_as_protocol()).await?
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
    };

    Ok(())
}
