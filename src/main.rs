use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                println!("accepted new connection");

                // Spawn a new task for handling the connection
                tokio::spawn(async {
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

    let mut line = String::new();
    while buf.read_line(&mut line).await? > 0 {
        if line.trim().is_empty() {
            break;
        }
        println!("line read: {}", line.trim_end());

        if line.contains("PING") {
            buf.get_mut().write_all("+PONG\r\n".as_bytes()).await?;
        }
        line.clear(); // Clear the buffer for the next line
    }

    Ok(())
}
