use std::{io::Write, net::TcpListener};

fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    
    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                println!("accepted new connection");

                match stream.write_all("+PONG\r\n".as_bytes()) {
                    Ok(()) => println!("response sent"),
                    Err(e) => println!("error sending response: {}", e),
                }
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
