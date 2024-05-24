use base64::prelude::*;

pub const EMPTY_RDB: &[u8] = b"UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==";

pub fn get_empty_rdb_decoded() -> Vec<u8> {
    BASE64_STANDARD.decode(EMPTY_RDB).unwrap()
}
