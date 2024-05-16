pub mod redis_command;
pub mod redis_runtime;
pub mod redis_type;
pub mod server_config;

pub trait RedisWritable {
    fn write_as_protocol(&self) -> Vec<u8>;
}
