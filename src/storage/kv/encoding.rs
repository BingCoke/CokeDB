//! Order-preserving encodings for use in keys.
//!
//! bool:    0x00 for false, 0x01 for true.
//! Vec<u8>: 0x00 is escaped with 0x00 0xff, terminated with 0x00 0x00.
//! String:  Like Vec<u8>.
//! u64:     Big-endian binary representation.
//! i64:     Big-endian binary representation, with sign bit flipped.
//! f64:     Big-endian binary representation, with sign bit flipped if +, all flipped if -.
//! Value:   Like above, with type prefix 0x00=Null 0x01=Boolean 0x02=Float 0x03=Integer 0x04=String
use crate::sql::Value;
use crate::errors::*;



use std::convert::TryInto;

/// 编解码 0x00 是 false , 0x01 是 true.
pub fn encode_boolean(bool: bool) -> u8 {
    if bool {
        0x01
    } else {
        0x00
    }
}

pub fn decode_boolean(byte: u8) -> Result<bool> {
    match byte {
        0x00 => Ok(false),
        0x01 => Ok(true),
        b => Err(Error::Encoding(format!("Invalid boolean value {:?}", b))),
    }
}

pub fn take_boolean(bytes: &mut &[u8]) -> Result<bool> {
    take_byte(bytes).and_then(decode_boolean)
}

pub fn encode_bytes(bytes: &[u8]) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(bytes.len() + 2);
    encoded.extend(
        bytes
            .iter()
            .flat_map(|b| match b {
                0x00 => vec![0x00, 0xff],
                b => vec![*b],
            })
            .chain(vec![0x00, 0x00]),
    );
    encoded
}

/// 获取一个byte 并且缩容切片
pub fn take_byte(bytes: &mut &[u8]) -> Result<u8> {
    if bytes.is_empty() {
        return Err(Error::Encoding("Unexpected end of bytes".into()));
    }
    let b = bytes[0];
    *bytes = &bytes[1..];
    Ok(b)
}

pub fn take_bytes(bytes: &mut &[u8]) -> Result<Vec<u8>> {
    let mut decoded = Vec::with_capacity(bytes.len() / 2);
    let mut iter = bytes.iter().enumerate();
    let taken = loop {
        match iter.next().map(|(_, b)| b) {
            Some(0x00) => match iter.next() {
                Some((i, 0x00)) => break i + 1,        // 0x00 0x00 is terminator
                Some((_, 0xff)) => decoded.push(0x00), // 0x00 0xff is escape sequence for 0x00
                Some((_, b)) => return Err(Error::Encoding(format!("Invalid byte escape {:?}", b))),
                None => return Err(Error::Encoding("Unexpected end of bytes".into())),
            },
            Some(b) => decoded.push(*b),
            None => return Err(Error::Encoding("Unexpected end of bytes".into())),
        }
    };
    *bytes = &bytes[taken..];
    Ok(decoded)
}

pub fn encode_f64(n: f64) -> [u8; 8] {
    let mut bytes = n.to_be_bytes();
    if bytes[0] >> 7 & 1 == 0 {
        bytes[0] ^= 1 << 7;
    } else {
        bytes.iter_mut().for_each(|b| *b = !*b);
    }
    bytes
}

pub fn decode_f64(mut bytes: [u8; 8]) -> f64 {
    if bytes[0] >> 7 & 1 == 1 {
        bytes[0] ^= 1 << 7;
    } else {
        bytes.iter_mut().for_each(|b| *b = !*b);
    }
    f64::from_be_bytes(bytes)
}

pub fn take_f64(bytes: &mut &[u8]) -> Result<f64> {
    if bytes.len() < 8 {
        return Err(Error::Encoding(format!("Unable to decode f64 from {} bytes", bytes.len())));
    }
    let n = decode_f64(bytes[0..8].try_into()?);
    *bytes = &bytes[8..];
    Ok(n)
}

pub fn encode_i64(n: i64) -> [u8; 8] {
    let mut bytes = n.to_be_bytes();
    bytes[0] ^= 1 << 7; // Flip left-most bit in the first byte, i.e. sign bit.
    bytes
}

pub fn decode_i64(mut bytes: [u8; 8]) -> i64 {
    bytes[0] ^= 1 << 7;
    i64::from_be_bytes(bytes)
}

pub fn take_i64(bytes: &mut &[u8]) -> Result<i64> {
    if bytes.len() < 8 {
        return Err(Error::Encoding(format!("Unable to decode i64 from {} bytes", bytes.len())));
    }
    let n = decode_i64(bytes[0..8].try_into()?);
    *bytes = &bytes[8..];
    Ok(n)
}

pub fn encode_string(string: &str) -> Vec<u8> {
    encode_bytes(string.as_bytes())
}

pub fn take_string(bytes: &mut &[u8]) -> Result<String> {
    Ok(String::from_utf8(take_bytes(bytes)?)?)
}

pub fn encode_u64(n: u64) -> [u8; 8] {
    n.to_be_bytes()
}

pub fn decode_u64(bytes: [u8; 8]) -> u64 {
    u64::from_be_bytes(bytes)
}

pub fn take_u64(bytes: &mut &[u8]) -> Result<u64> {
    if bytes.len() < 8 {
        return Err(Error::Encoding(format!("Unable to decode u64 from {} bytes", bytes.len())));
    }
    let n = decode_u64(bytes[0..8].try_into()?);
    *bytes = &bytes[8..];
    Ok(n)
}

/// 编码一个value
pub fn encode_value(value: &Value) -> Vec<u8> {
    match value {
        Value::Null => vec![0x00],
        Value::Bool(b) => vec![0x01, encode_boolean(*b)],
        Value::Float(f) => [&[0x02][..], &encode_f64(*f)].concat(),
        Value::Integer(i) => [&[0x03][..], &encode_i64(*i)].concat(),
        Value::String(s) => [&[0x04][..], &encode_string(s)].concat(),
    }
}

/// 通过u8数组 获得Value
pub fn take_value(bytes: &mut &[u8]) -> Result<Value> {
    match take_byte(bytes)? {
        0x00 => Ok(Value::Null),
        0x01 => Ok(Value::Bool(take_boolean(bytes)?)),
        0x02 => Ok(Value::Float(take_f64(bytes)?)),
        0x03 => Ok(Value::Integer(take_i64(bytes)?)),
        0x04 => Ok(Value::String(take_string(bytes)?)),
        n => Err(Error::Encoding(format!("Invalid value prefix {:x?}", n))),
    }
}


