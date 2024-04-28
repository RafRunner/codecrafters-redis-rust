use std::{
    io::{BufRead, BufReader, Write},
    net::{TcpListener, TcpStream},
};

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
    let mut buf = BufReader::new(&mut stream);

    let mut line = String::new();
    while buf.read_line(&mut line)? > 0 {
        if line.trim().is_empty() {
            break;
        }
        println!("line read: {}", line.trim_end());
        buf.get_mut().write_all("+PONG\r\n".as_bytes())?;
        line.clear(); // Clear the buffer for the next line
    }

    Ok(())
}
