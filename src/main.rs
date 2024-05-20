use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;

use redis_starter_rust::redis_runtime::RedisRuntime;
use redis_starter_rust::server_config::ServerConfig;
use redis_starter_rust::{redis_command::RedisCommand, redis_type::RedisType, RedisWritable};
use std::env;
use tokio::io::{AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};

#[tokio::main]
async fn main() {
    let args: Vec<String> = env::args().collect();
    let config = ServerConfig::parse_command_line_args(&args);

    let listen_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), config.port);
    let listener = TcpListener::bind(listen_addr).await.unwrap();

    println!("Listening on port {}", config.port);

    let runtime = Arc::new(RedisRuntime::new(config));
    runtime
        .perform_handshake()
        .await
        .expect("Error during handshake");

    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                println!("Accepted new connection");
                let runtime_clone = Arc::clone(&runtime);

                // Spawn a new task for handling the connection
                tokio::spawn(async move {
                    match handle_connection(stream, runtime_clone.as_ref()).await {
                        Ok(()) => println!("Connection handled successfully"),
                        Err(e) => println!("Error handling connection: {}", e),
                    }
                });
            }
            Err(e) => println!("Error accepting connection: {}", e),
        }
    }
}

async fn handle_connection(
    mut stream: TcpStream,
    runtime: &RedisRuntime,
) -> Result<(), anyhow::Error> {
    let mut buf = BufReader::new(&mut stream);

    loop {
        match RedisType::parse(&mut buf).await {
            Ok(Some(input)) => {
                println!("Input type: {:?}", input);

                match RedisCommand::parse(&input) {
                    Some(command) => {
                        println!("Executing command: {:?}", command);
                        let result = runtime.execute(command).await;
                        println!("Command result: {:?}", result);

                        buf.write_all(&result.write_as_protocol()).await?;
                    }
                    None => {
                        println!("No response built");
                        let err = RedisType::simple_error("Unrecognized command");
                        buf.write_all(&err.write_as_protocol()).await?;
                    }
                }
            }
            Ok(None) => break,
            Err(err) => {
                println!("Error parsing input type: {:?}", &err);
                let err = RedisType::simple_error(&err.to_string());
                buf.write_all(&err.write_as_protocol()).await?;
            }
        }
    }

    Ok(())
}
