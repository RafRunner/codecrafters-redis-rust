use std::{io::{BufRead, BufReader, Write}, net::{TcpListener, TcpStream}};

fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                println!("accepted new connection");

                match handle_connection(stream) {
                    Ok(()) => println!("connection handled successfully"),
                    Err(e) => println!("error handling connection: {}", e),
                }
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

fn handle_connection(mut stream: TcpStream) -> Result<(), anyhow::Error> {
    let buf = BufReader::new(&mut stream);

    for line in buf.lines().collect::<Vec<_>>() {
        let line = line?;
        println!("line read: {}", &line);
        stream.write_all("+PONG\r\n".as_bytes())?;
    }

    Ok(())
}
