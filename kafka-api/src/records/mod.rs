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

use std::{
    cell::OnceCell,
    fmt::{Debug, Formatter},
};

use bytes::Buf;
pub use consts::*;
pub use mutable_batches::*;
pub use readonly_batches::*;
pub use record::*;
pub use record_batch::*;

use crate::bytebuffer::ByteBuffer;

mod consts;
mod mutable_batches;
mod readonly_batches;
mod record;
mod record_batch;
const BATCH_EXPIRATION: i64 = 3600 * 1000; // 15 seconds.

fn load_batches(buf: &ByteBuffer) -> Vec<RecordBatch> {
    let mut batches = vec![];
    let expiration = chrono::Utc::now().timestamp_millis() + BATCH_EXPIRATION;

    let mut offset = 0;
    let mut remaining = buf.len() - offset;
    while remaining > 0 {
        assert!(
            remaining >= HEADER_SIZE_UP_TO_MAGIC,
            "no enough bytes when decode records (remaining: {})",
            remaining
        );

        let record_size = (&buf[LENGTH_OFFSET..]).get_i32();
        let batch_size = record_size as usize + LOG_OVERHEAD;

        assert!(
            remaining >= batch_size,
            "no enough bytes when decode records (remaining: {})",
            remaining
        );

        let record = match (&buf[MAGIC_OFFSET..]).get_i8() {
            2 => {
                let buf = buf.slice(offset..offset + batch_size);
                offset += batch_size;
                remaining -= batch_size;
                let base_offset = buf.slice(BASE_OFFSET_OFFSET..).get_i64();
                let last_offset_delta = buf.slice(LAST_OFFSET_DELTA_OFFSET..).get_i32() as i64;
                RecordBatch::new(buf, expiration, base_offset, last_offset_delta)
            }
            v => unimplemented!("record batch version {}", v),
        };

        batches.push(record);
    }

    batches
}
