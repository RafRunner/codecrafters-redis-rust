// use redis_starter_rust::redis_client::RedisClient;
use redis_starter_rust::redis_command::RedisCommand;
use redis_starter_rust::redis_runtime::RedisRuntime;
use redis_starter_rust::redis_type::RedisType;
use redis_starter_rust::server_config::ServerConfig;
use redis_starter_rust::RedisWritable;
use std::cmp::min;
use std::env;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{split, AsyncWriteExt, BufReader, ReadHalf, WriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, Mutex};
use tokio::task::JoinHandle;

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
    let runtime_clone = Arc::clone(&runtime);
    tokio::spawn(set_up_replica_loop(runtime_clone));

    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                println!("Accepted new connection");
                let runtime_clone = Arc::clone(&runtime);
                let _ = handle_connection(stream, runtime_clone, false);
            }
            Err(e) => println!("Error accepting connection: {}", e),
        }
    }
}

async fn set_up_replica_loop(runtime: Arc<RedisRuntime>) {
    let mut backoff = Duration::from_secs(1);

    loop {
        match runtime.perform_handshake().await {
            Ok(Some(stream)) => {
                println!("Setting up connection handlers as a replica.");
                backoff = Duration::from_secs(1);

                let runtime_clone = Arc::clone(&runtime);
                if let Ok((read_handle, write_handle)) =
                    handle_connection(stream, runtime_clone, true)
                {
                    // Join the read and write tasks. If either fails, we try to reconnect.
                    let _ = tokio::join!(read_handle, write_handle);
                    println!("Connection to master lost. Reconnecting in {:?}", backoff);
                }
            }
            Ok(None) => break, // Exit the loop since the instance is a master.
            Err(e) => {
                println!("Error during handshake: {e}");
            }
        }

        // If the instance is a replica and lost connection, or couldn't connect, retry with backoff
        println!("Retrying in {:?}", backoff);
        tokio::time::sleep(backoff).await;
        backoff = min(backoff * 2, Duration::from_secs(30)); // Exponential backoff capped at 30 seconds
    }
}

#[derive(Debug)]
enum CommandOrError {
    Command(RedisCommand),
    Error(anyhow::Error),
}

fn handle_connection(
    stream: TcpStream,
    runtime: Arc<RedisRuntime>,
    from_master: bool,
) -> anyhow::Result<(JoinHandle<()>, JoinHandle<anyhow::Result<()>>)> {
    let peer_ip = stream.peer_addr()?.ip();
    let (read_half, write_half) = split(stream);
    let (tx, rx) = mpsc::channel(32);

    // Spawn task to handle reading
    let read_handle = tokio::spawn(handle_reading(read_half, tx));

    // Spawn task to handle processing and writing
    let write_handle = tokio::spawn(handle_processing_writing(
        rx,
        write_half,
        runtime,
        from_master,
        peer_ip,
    ));

    Ok((read_handle, write_handle))
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
    peer_ip: IpAddr,
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
                let result = runtime
                    .execute(&command, Some((peer_ip, write_clone)))
                    .await;
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
