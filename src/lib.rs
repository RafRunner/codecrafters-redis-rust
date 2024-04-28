use async_recursion::async_recursion;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, BufReader};

pub trait RedisWritable {
    fn write_as_protocol(&self) -> Vec<u8>;
}

#[derive(Debug, PartialEq, Eq)]
pub enum RedisType {
    List {
        len: usize,
        data: Vec<Box<RedisType>>,
    },
    BulkString {
        len: usize,
        data: String,
    },
    SimpleString {
        data: String,
    },
    SimpleError {
        message: String,
    },
}

impl RedisType {
    #[async_recursion]
    pub async fn parse(
        reader: &mut BufReader<impl AsyncRead + Unpin + Send>,
    ) -> Result<Self, anyhow::Error> {
        let mut command_char = [0; 1];
        reader.read_exact(&mut command_char).await?;

        Ok(match command_char[0] as char {
            '*' => {
                let len = Self::read_len(reader).await?;
                let mut elements = Vec::new();

                for _ in 0..len {
                    let element = Self::parse(reader).await?;
                    elements.push(Box::new(element));
                }

                Self::List {
                    len,
                    data: elements,
                }
            }
            '$' => {
                let len = Self::read_len(reader).await?;
                let mut buf = String::new();

                for _ in 0..len {
                    let byte = reader.read_u8().await?;
                    buf.push(byte as char);
                }
                let mut unwanted_space = Vec::new();
                reader.read_until(b'\n', &mut unwanted_space).await?;

                Self::BulkString { len, data: buf }
            }
            '+' => {
                let mut line = String::new();
                reader.read_line(&mut line).await?;

                Self::SimpleString {
                    data: line[0..line.len() - 2].to_string(),
                }
            }
            _char => {
                todo!("Handle simples string command/errors")
            }
        })
    }

    async fn read_len(
        reader: &mut BufReader<impl AsyncRead + Unpin>,
    ) -> Result<usize, anyhow::Error> {
        let mut line = String::new();
        reader.read_line(&mut line).await?;

        let line: usize = line.trim_end().parse()?;

        Ok(line)
    }
}

impl RedisWritable for RedisType {
    fn write_as_protocol(&self) -> Vec<u8> {
        match self {
            RedisType::List { len, data } => {
                let mut bytes = Vec::new();

                for &byte in format!("*{}\r\n", len).as_bytes() {
                    bytes.push(byte);
                }

                for elem in data {
                    bytes.append(&mut elem.write_as_protocol());
                }

                bytes
            }
            RedisType::BulkString { len, data } => {
                format!("${}\r\n{}\r\n", len, data).as_bytes().to_vec()
            }
            RedisType::SimpleString { data } => format!("+{}\r\n", data).as_bytes().to_vec(),
            RedisType::SimpleError { message } => format!("-{}\r\n", message).as_bytes().to_vec(),
        }
    }
}

#[derive(Debug)]
pub enum RedisCommand {
    PING,
    ECHO(String),
}

impl RedisWritable for RedisCommand {
    fn write_as_protocol(&self) -> Vec<u8> {
        match self {
            RedisCommand::PING => RedisType::SimpleString {
                data: "PONG".to_string(),
            }
            .write_as_protocol(),
            RedisCommand::ECHO(payload) => RedisType::BulkString {
                len: payload.len(),
                data: payload.clone(),
            }
            .write_as_protocol(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn create_buf_reader<'a>(data: &'a str) -> BufReader<impl AsyncRead + Unpin + 'a> {
        BufReader::new(Cursor::new(data))
    }

    async fn assert_type_equals(input: &str, expected: RedisType) {
        let parsed = RedisType::parse(&mut create_buf_reader(input))
            .await
            .unwrap();

        assert_eq!(expected, parsed);
    }

    #[tokio::test]
    async fn test_parse_simple_string() {
        let input = "+PING\r\n";
        let expected = RedisType::SimpleString {
            data: "PING".to_string(),
        };

        assert_type_equals(input, expected).await
    }

    #[tokio::test]
    async fn test_parse_bulk_string() {
        let input = "$11\r\nHello\nWorld\r\n";
        let expected = RedisType::BulkString {
            len: 11,
            data: "Hello\nWorld".to_string(),
        };

        assert_type_equals(input, expected).await
    }

    #[tokio::test]
    async fn test_parse_list() {
        let input = "*2\r\n$3\r\nfoo\r\n$4\r\nbarr\r\n";
        let expected = RedisType::List {
            len: 2,
            data: vec![
                Box::new(RedisType::BulkString {
                    len: 3,
                    data: "foo".to_string(),
                }),
                Box::new(RedisType::BulkString {
                    len: 4,
                    data: "barr".to_string(),
                }),
            ],
        };

        assert_type_equals(input, expected).await
    }
}
