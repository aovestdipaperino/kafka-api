// Copyright 2023 tison <wander4096@gmail.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::hash::Hasher;
use std::io;
use crate::bytebuffer::ByteBuffer;
use crate::codec::{Encoder, NullableBytes32};
use crate::records::ReadOnlyBatches;
use crate::Writable;

#[derive(Debug, Default, Clone)]
pub struct Record {
    pub len: i32, // varint
    /// bit 0~7: unused
    pub attributes: i8,
    pub timestamp_delta: i64, // varlong
    pub offset_delta: i32,    // varint
    pub key_len: i32,         // varint
    pub key: Option<ByteBuffer>,
    pub value_len: i32, // varint
    pub value: Option<ByteBuffer>,
    pub headers: Vec<Header>,
}

impl Encoder<&Record> for Record {
    fn encode<B: Writable>(&self, buffer: &mut B, value: &Record) -> io::Result<()>{
        buffer.write_varint(value.len).expect("TODO: panic message");
        buffer.write_i8(value.attributes).expect("TODO: panic message");
        buffer.write_varlong(value.timestamp_delta).expect("TODO: panic message");
        buffer.write_varint(value.offset_delta).expect("TODO: panic message");
        buffer.write_varint(value.key_len).expect("TODO: panic message");
        if let Some(key) = &value.key {
            buffer.write_bytes(key).expect("TODO: panic message");
        }
        buffer.write_varint(value.value_len).expect("TODO: panic message");
        if let Some(value) = &value.value {
            buffer.write_bytes(value).expect("TODO: panic message");
        }
        buffer.write_varint(value.headers.len() as i32).expect("TODO: panic message");
        for header in &value.headers {
            buffer.write_varint(header.key_len).expect("TODO: panic message");
            if let Some(key) = &header.key {
                buffer.write_bytes(key).expect("TODO: panic message");
            }
            buffer.write_varint(header.value_len).expect("TODO: panic message");
            if let Some(value) = &header.value {
                buffer.write_bytes(value).expect("TODO: panic message");
            }
        };
        Ok(())
    }

    fn calculate_size(&self, value: &Record) -> usize {
        // let mut size = 0;
        // size += self.len.len();
        // size += 1;
        // size += self.timestamp_delta.len();
        // size += self.offset_delta.len();
        // size += self.key_len.len();
        // if let Some(key) = &self.key {
        //     size += key.len();
        // }
        // size += self.value_len.len();
        // if let Some(value) = &self.value {
        //     size += value.len();
        // }
        // size += self.headers.len().len();
        // for header in &self.headers {
        //     size += header.key_len.len();
        //     if let Some(key) = &header.key {
        //         size += key.len();
        //     }
        //     size += header.value_len.len();
        //     if let Some(value) = &header.value {
        //         size += value.len();
        //     }
        // }
        0
    }
}

#[derive(Debug, Default, Clone)]
pub struct Header {
    pub key_len: i32, // varint
    pub key: Option<ByteBuffer>,
    pub value_len: i32, // varint
    pub value: Option<ByteBuffer>,
}

#[derive(Debug, Clone, Copy)]
pub enum TimestampType {
    CreateTime,
    LogAppendTime,
}

#[derive(Debug, Default, Clone, Copy)]
pub enum CompressionType {
    #[default]
    None,
    Gzip,
    Snappy,
    Lz4,
    Zstd,
}

impl From<u8> for CompressionType {
    fn from(ty: u8) -> Self {
        match ty {
            0 => CompressionType::None,
            1 => CompressionType::Gzip,
            2 => CompressionType::Snappy,
            3 => CompressionType::Lz4,
            4 => CompressionType::Zstd,
            _ => unreachable!("Unknown compression type id: {}", ty),
        }
    }
}
