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

async fn handle_reading(read_half: ReadHalf<TcpStream>, tx: mpsc::Sender<RedisCommand>) {
    let mut buf = BufReader::new(read_half);

    loop {
        let command = RedisType::parse(&mut buf).await;

        match command {
            Ok(Some(input)) => {
                println!("Input type: {:?}", input);

                match RedisCommand::parse(&input) {
                    Some(command) => {
                        if let Err(e) = tx.send(command).await {
                            println!("Error sending command to processor. {}", e);
                            break;
                        }
                    }
                    None => {
                        println!("No response built");
                        // let err = RedisType::simple_error("Unrecognized command");
                        // buf.lock().await.write_all(&err.write_as_protocol()).await?;
                    }
                }
            }
            Ok(None) => break,
            Err(err) => {
                println!("Error parsing input type: {:?}", &err);
                // let err = RedisType::simple_error(&err.to_string());
                // buf.lock().await.write_all(&err.write_as_protocol()).await?;
            }
        }
    }
}

async fn handle_processing_writing(
    mut rx: mpsc::Receiver<RedisCommand>,
    write_half: WriteHalf<TcpStream>,
    runtime: Arc<RedisRuntime>,
    from_master: bool,
) -> Result<(), anyhow::Error> {
    let write_half = Arc::new(Mutex::new(write_half));

    while let Some(command) = rx.recv().await {
        let write_clone = Arc::clone(&write_half);
        println!("Executing command: {:?}", command);

        if !runtime.is_master() && command.is_write_command() && !from_master {
            let error = RedisType::simple_error("You can't write against a read only replica.");

            let mut write_guard = write_half.lock().await;
            write_guard.write_all(&error.write_as_protocol()).await?;
            continue;
        }

        let result = runtime.execute(&command, Some(write_clone)).await;
        println!("Command result: {:?}", result);

        if runtime.is_master() || !command.is_write_command() {
            let mut write_guard = write_half.lock().await;
            write_guard.write_all(&result.write_as_protocol()).await?;
        }

        if let Err(e) = runtime.replicate_command(&command).await {
            println!("Error replicating command: {}", e);
        }
    }

    Ok(())
}
