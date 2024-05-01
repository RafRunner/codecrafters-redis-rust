pub mod redis_command;
pub mod redis_runtime;
pub mod redis_type;

pub trait RedisWritable {
    fn write_as_protocol(&self) -> Vec<u8>;
}
