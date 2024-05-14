use std::io::ErrorKind;

use async_recursion::async_recursion;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, BufReader};

use crate::RedisWritable;

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum RedisType {
    List { data: Vec<Box<RedisType>> },
    BulkString { data: String },
    SimpleString { data: String },
    NullBulkString,
    SimpleError { message: String },
}

impl RedisType {
    #[async_recursion]
    pub async fn parse(
        reader: &mut BufReader<impl AsyncRead + Unpin + Send>,
    ) -> Result<Option<Self>, anyhow::Error> {
        let command_char = match reader.read_u8().await {
            Ok(byte) => Ok(byte as char),
            Err(e) => {
                if e.kind() == ErrorKind::UnexpectedEof {
                    return Ok(None); // No more data from client
                }
                Err(e)
            }
        }?;

        Ok(Some(match command_char {
            '*' => {
                let len = Self::read_len(reader).await?;
                let mut elements = Vec::new();

                for _ in 0..len {
                    if let Some(element) = Self::parse(reader).await? {
                        elements.push(Box::new(element));
                    }
                }

                Self::List { data: elements }
            }
            '$' => {
                let len = Self::read_len(reader).await?;
                let mut buffer = vec![0; len + 2]; // +2 for CRLF
                reader.read_exact(&mut buffer).await?;
                let data = String::from_utf8(buffer[..len].to_vec())?;

                Self::BulkString { data }
            }
            '+' => {
                let mut line = String::new();
                reader.read_line(&mut line).await?;
                line.truncate(line.len() - 2); // Removing CRLF

                Self::SimpleString { data: line }
            }
            character => Self::SimpleError {
                message: format!("Unknown command {}", character),
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
            RedisType::List { data } => {
                let mut bytes = Vec::new();

                for &byte in format!("*{}\r\n", data.len()).as_bytes() {
                    bytes.push(byte);
                }

                for elem in data {
                    bytes.append(&mut elem.write_as_protocol());
                }

                bytes
            }
            RedisType::BulkString { data } => format!("${}\r\n{}\r\n", data.len(), data)
                .as_bytes()
                .to_vec(),
            RedisType::NullBulkString => b"$-1\r\n".to_vec(),
            RedisType::SimpleString { data } => format!("+{}\r\n", data).as_bytes().to_vec(),
            RedisType::SimpleError { message } => format!("-{}\r\n", message).as_bytes().to_vec(),
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
            data: "Hello\nWorld".to_string(),
        };

        assert_type_equals(input, expected).await
    }

    #[tokio::test]
    async fn test_parse_list() {
        let input = "*2\r\n$3\r\nfoo\r\n$4\r\nbarr\r\n";
        let expected = RedisType::List {
            data: vec![
                Box::new(RedisType::BulkString {
                    data: "foo".to_string(),
                }),
                Box::new(RedisType::BulkString {
                    data: "barr".to_string(),
                }),
            ],
        };

        assert_type_equals(input, expected).await
    }
}
