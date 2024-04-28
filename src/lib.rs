use std::io::ErrorKind;

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
    ) -> Result<Option<Self>, anyhow::Error> {
        let mut command_char = [0; 1];
        if let Err(e) = reader.read_exact(&mut command_char).await {
            if e.kind() == ErrorKind::UnexpectedEof {
                return Ok(None); // No more data from client
            }
        }

        Ok(Some(match command_char[0] as char {
            '*' => {
                let len = Self::read_len(reader).await?;
                let mut elements = Vec::new();

                for _ in 0..len {
                    if let Some(element) = Self::parse(reader).await? {
                        elements.push(Box::new(element));
                    }
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
            character => Self::SimpleError {
                message: format!("Unknow command {}", character),
            },
        }))
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

#[derive(Debug, PartialEq, Eq)]
pub enum RedisCommand {
    PING,
    ECHO(String),
}

impl RedisCommand {
    pub fn parse(data: &RedisType) -> Result<Option<RedisCommand>, anyhow::Error> {
        match data {
            RedisType::List { len, data } => {
                let vector = data;

                match len {
                    2 => {
                        if let Some(data) = Self::extract_string(&vector[0]) {
                            if data.to_lowercase() == "echo" {
                                if let Some(data) = Self::extract_string(&vector[1]) {
                                    return Ok(Some(RedisCommand::ECHO(data.to_string())));
                                }
                            }
                        }
                    }
                    1 => {
                        return Self::parse(&vector[0]);
                    }
                    _ => (),
                }
            }
            RedisType::BulkString { data, .. } | RedisType::SimpleString { data, .. } => {
                if data.to_lowercase() == "ping" {
                    return Ok(Some(RedisCommand::PING));
                }
            }
            RedisType::SimpleError { .. } => (), // Do nothing
        }

        Ok(None)
    }

    pub fn execute(&self) -> RedisType {
        match self {
            RedisCommand::PING => RedisType::SimpleString {
                data: "PONG".to_string(),
            },
            RedisCommand::ECHO(payload) => RedisType::BulkString {
                len: payload.len(),
                data: payload.clone(),
            },
        }
    }

    fn extract_string(redis_type: &RedisType) -> Option<&str> {
        match redis_type {
            RedisType::BulkString { data, .. } | RedisType::SimpleString { data, .. } => {
                Some(&data)
            }
            _ => None,
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

        assert_eq!(Some(expected), parsed);
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

    #[test]
    fn test_parse_ping() {
        let data = RedisType::SimpleString {
            data: "PING".to_string(),
        };

        let result = RedisCommand::parse(&data).unwrap();
        assert_eq!(result, Some(RedisCommand::PING));

        let data = RedisType::List {
            len: 1,
            data: vec![Box::new(RedisType::BulkString {
                len: 4,
                data: "Ping".to_string(),
            })],
        };

        let result = RedisCommand::parse(&data).unwrap();
        assert_eq!(result, Some(RedisCommand::PING));
    }

    #[test]
    fn test_parse_echo() {
        let data = RedisType::List {
            len: 2,
            data: vec![
                Box::new(RedisType::BulkString {
                    len: 4,
                    data: "echo".to_string(),
                }),
                Box::new(RedisType::BulkString {
                    len: 5,
                    data: "hello".to_string(),
                }),
            ],
        };

        let result = RedisCommand::parse(&data).unwrap();
        assert_eq!(result, Some(RedisCommand::ECHO("hello".to_string())));
    }

    #[test]
    fn test_parse_invalid() {
        let data = RedisType::List {
            len: 2,
            data: vec![
                Box::new(RedisType::BulkString {
                    len: 7,
                    data: "invalid".to_string(),
                }),
                Box::new(RedisType::BulkString {
                    len: 5,
                    data: "world".to_string(),
                }),
            ],
        };

        let result = RedisCommand::parse(&data).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_parse_empty() {
        let data = RedisType::SimpleString {
            data: "".to_string(),
        };

        let result = RedisCommand::parse(&data).unwrap();
        assert_eq!(result, None);
    }
}
