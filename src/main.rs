// use redis_starter_rust::redis_client::RedisClient;
use redis_starter_rust::redis_command::RedisCommand;
use redis_starter_rust::redis_runtime::RedisRuntime;
use redis_starter_rust::redis_type::RedisType;
use redis_starter_rust::server_config::ServerConfig;
use redis_starter_rust::RedisWritable;
use std::env;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use tokio::io::{split, AsyncWriteExt, BufReader, ReadHalf, WriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, Mutex};

#[tokio::main]
async fn main() {
    // let mut client = RedisClient::new(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 6379)).await.unwrap();
    // let command = RedisCommand::SET { key: "foo".to_string(), val: RedisType::simple_string("bar"), ttl: None };
    // let result = client.send_command(&command).await;
    // println!("Result: {:?}", result);
    let args: Vec<String> = env::args().collect();
    let config = ServerConfig::parse_command_line_args(&args);

    let listen_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), config.port);
    let listener = TcpListener::bind(listen_addr).await.unwrap();
    println!("Listening on port {}", config.port);

    let runtime = Arc::new(RedisRuntime::new(config));
    let master_conn = runtime
        .perform_handshake()
        .await
        .expect("Error during handshake");

    if let Some(stream) = master_conn {
        let runtime_clone = Arc::clone(&runtime);
        handle_connection(stream, runtime_clone, true);
    }

    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                println!("Accepted new connection");
                let runtime_clone = Arc::clone(&runtime);
                handle_connection(stream, runtime_clone, false);
            }
            Err(e) => println!("Error accepting connection: {}", e),
        }
    }
}

#[derive(Debug)]
enum CommandOrError {
    Command(RedisCommand),
    Error(anyhow::Error),
}

fn handle_connection(stream: TcpStream, runtime: Arc<RedisRuntime>, from_master: bool) {
    let (read_half, write_half) = split(stream);
    let (tx, rx) = mpsc::channel(32);

    // Spawn task to handle reading
    tokio::spawn(handle_reading(read_half, tx));

    // Spawn task to handle processing and writing
    tokio::spawn(handle_processing_writing(
        rx,
        write_half,
        runtime,
        from_master,
    ));
}

async fn handle_reading(read_half: ReadHalf<TcpStream>, tx: mpsc::Sender<CommandOrError>) {
    let mut buf = BufReader::new(read_half);

    loop {
        let command = RedisType::parse(&mut buf).await;

        match command {
            Ok(Some(input)) => {
                println!("Input type: {:?}", input);

                match RedisCommand::parse(&input) {
                    Some(command) => {
                        tx.send(CommandOrError::Command(command)).await.unwrap();
                    }
                    None => {
                        tx.send(CommandOrError::Error(anyhow::anyhow!(
                            "Not a valid command: {:?}",
                            input
                        )))
                        .await
                        .unwrap();
                    }
                }
            }
            Ok(None) => break,
            Err(err) => {
                tx.send(CommandOrError::Error(anyhow::anyhow!(
                    "Error parsing input type: {:?}",
                    &err
                )))
                .await
                .unwrap();
            }
        }
    }
}

async fn handle_processing_writing(
    mut rx: mpsc::Receiver<CommandOrError>,
    write_half: WriteHalf<TcpStream>,
    runtime: Arc<RedisRuntime>,
    from_master: bool,
) -> Result<(), anyhow::Error> {
    let write_half = Arc::new(Mutex::new(write_half));

    while let Some(command_or_error) = rx.recv().await {
        match command_or_error {
            CommandOrError::Command(command) => {
                let write_clone = Arc::clone(&write_half);

                if !runtime.is_master() && command.is_write_command() && !from_master {
                    let error_msg = "You can't write against a read only replica.";
                    println!("{}", error_msg);
                    let error = RedisType::simple_error(error_msg);

                    write_half
                        .lock()
                        .await
                        .write_all(&error.write_as_protocol())
                        .await?;
                    continue;
                }

                println!("Executing command: {:?}", command);
                let result = runtime.execute(&command, Some(write_clone)).await;
                println!("Command result: {:?}", result);

                if runtime.is_master() || !command.is_write_command() {
                    write_half
                        .lock()
                        .await
                        .write_all(&result.write_as_protocol())
                        .await?;
                }

                if let Err(e) = runtime.replicate_command(&command).await {
                    println!("Error replicating command: {}", e);
                }
            }
            CommandOrError::Error(error) => {
                println!("Recieved error from channel: {}. Sending error back", error);
                let error = RedisType::simple_error(&error.to_string());
                write_half
                    .lock()
                    .await
                    .write_all(&error.write_as_protocol())
                    .await?;
            }
        }
    }

    Ok(())
}
