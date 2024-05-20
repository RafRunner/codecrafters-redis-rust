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
                let len: u64 = Self::read_line(reader).await?.parse()?;
                let mut elements = Vec::new();

                for _ in 0..len {
                    if let Some(element) = Self::parse(reader).await? {
                        elements.push(Box::new(element));
                    }
                }

                Self::List { data: elements }
            }
            '$' => {
                let len: i64 = Self::read_line(reader).await?.parse()?;
                if len == -1 {
                    Self::NullBulkString
                } else if len < 0 {
                    return Err(anyhow::anyhow!("Invalid bulk string len ({})!", len));
                } else {
                    let len = len as usize;

                    let mut buffer = vec![0; len + 2]; // +2 for CRLF
                    reader.read_exact(&mut buffer).await?;
                    let data = String::from_utf8(buffer[..len].to_vec())?;

                    Self::BulkString { data }
                }
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

    pub fn extract_string(&self) -> Option<&str> {
        match self {
            RedisType::BulkString { data, .. } | RedisType::SimpleString { data, .. } => Some(data),
            _ => None,
        }
    }

    pub fn expect_string(
        &self,
        expected: &str,
        message_start: &str,
    ) -> Result<&str, anyhow::Error> {
        match self.extract_string() {
            Some(string) if expected.to_lowercase() == string.to_lowercase() => Ok(string),
            _ => Err(anyhow::anyhow!(
                "{}. Expected string {}, received: {:?}",
                message_start,
                expected,
                self
            )),
        }
    }

    pub fn simple_string(data: &str) -> Self {
        RedisType::SimpleString {
            data: data.to_string(),
        }
    }

    pub fn bulk_string(data: &str) -> Self {
        RedisType::BulkString {
            data: data.to_string(),
        }
    }

    pub fn list(data: Vec<Self>) -> Self {
        RedisType::List {
            data: data.into_iter().map(Box::new).collect(),
        }
    }

    pub fn simple_error(message: &str) -> Self {
        RedisType::SimpleError {
            message: message.to_string(),
        }
    }

    async fn read_line(
        reader: &mut BufReader<impl AsyncRead + Unpin>,
    ) -> Result<String, anyhow::Error> {
        let mut line = String::new();
        reader.read_line(&mut line).await?;

        Ok(line.trim_end().to_string())
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
        let expected = RedisType::simple_string("PING");

        assert_type_equals(input, expected).await
    }

    #[tokio::test]
    async fn test_parse_bulk_string() {
        let input = "$11\r\nHello\nWorld\r\n";
        let expected = RedisType::bulk_string("Hello\nWorld");

        assert_type_equals(input, expected).await
    }

    #[tokio::test]
    async fn test_parse_null_bulk_string() {
        let input = "$-1\r\n";
        let expected = RedisType::NullBulkString;

        assert_type_equals(input, expected).await
    }

    #[tokio::test]
    async fn test_parse_list() {
        let input = "*2\r\n$3\r\nfoo\r\n$4\r\nbarr\r\n";
        let expected = RedisType::list(vec![
            RedisType::bulk_string("foo"),
            RedisType::bulk_string("barr"),
        ]);

        assert_type_equals(input, expected).await
    }
}
