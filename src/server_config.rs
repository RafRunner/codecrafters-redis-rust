use std::net::{SocketAddr, ToSocketAddrs};

#[derive(Debug, PartialEq, Eq)]
pub struct ServerConfig {
    pub port: u16,
    pub replica_addr: Option<SocketAddr>,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            port: 6379,
            replica_addr: None,
        }
    }
}

impl ServerConfig {
    pub fn parse_command_line_args(args: &[String]) -> Self {
        let mut initial_config = Self::default();
        let mut args_iter = args.iter();

        while let Some(arg) = args_iter.next() {
            if arg == "--port" {
                if let Some(p) = args_iter.next() {
                    initial_config.port = p
                        .parse()
                        .unwrap_or_else(|_| panic!("Invalid port number provided: {}", p));
                } else {
                    panic!("Please provide a port value");
                }
            } else if arg == "--replicaof" {
                if let Some(addr) = args_iter.next() {
                    match addr.replace(' ', ":").to_socket_addrs() {
                        Ok(mut addrs) => {
                            if let Some(address) = addrs.next() {
                                initial_config.replica_addr = Some(address);
                            } else {
                                panic!("No valid addresses found for the provided replica address");
                            }
                        }
                        Err(_) => panic!("Invalid address specified: {}", addr),
                    }
                } else {
                    panic!("Please provide a master address");
                }
            }
        }

        initial_config
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_with_default_port() {
        let args = vec![];
        let config = ServerConfig::parse_command_line_args(&args);
        assert_eq!(
            config,
            ServerConfig {
                port: 6379,
                replica_addr: None
            }
        );
    }

    #[test]
    fn test_parse_custom_port() {
        let args = vec!["--port".to_string(), "8080".to_string()];
        let config = ServerConfig::parse_command_line_args(&args);
        assert_eq!(
            config,
            ServerConfig {
                port: 8080,
                replica_addr: None
            }
        );
    }

    #[test]
    #[should_panic(expected = "Invalid port number provided: invalid_port")]
    fn test_parse_invalid_port() {
        let args = vec!["--port".to_string(), "invalid_port".to_string()];
        let _config = ServerConfig::parse_command_line_args(&args);
    }

    #[test]
    fn test_parse_replica_of() {
        let args = vec!["--replicaof".to_string(), "192.168.1.2 6000".to_string()];
        let config = ServerConfig::parse_command_line_args(&args);
        let expected_addr = "192.168.1.2:6000".parse().unwrap();
        assert_eq!(
            config,
            ServerConfig {
                port: 6379,
                replica_addr: Some(expected_addr)
            }
        );
    }

    #[test]
    fn test_parse_replica_and_port() {
        let args = vec![
            "--replicaof".to_string(),
            "localhost 6379".to_string(),
            "--port".to_string(),
            "8333".to_string(),
        ];
        let config = ServerConfig::parse_command_line_args(&args);
        let expected_addr = "localhost:6379".to_socket_addrs().unwrap().next().unwrap();
        assert_eq!(
            config,
            ServerConfig {
                port: 8333,
                replica_addr: Some(expected_addr)
            }
        );
    }
}
