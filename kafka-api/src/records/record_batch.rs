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

use std::fmt::{Debug, Formatter};
use std::io::Write;
use bytes::{Buf, BufMut, BytesMut};
use crc::Crc;
use tracing::error;
use uuid::Bytes;
use crate::{
    bytebuffer::ByteBuffer,
    codec::{Decoder, RecordList},
    records::*,
};
use crate::codec::Encoder;
use crate::sendable::SendBuilder;

#[derive(Default)]
pub struct RecordBatch {
    //pub(super) buf: ByteBuffer,
    base_offset: i64,
    last_offset_delta: i64,
    batch_size: usize,
    pub expiration: i64,
    pub records: Vec<Record>,
}


impl Debug for RecordBatch {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut de = f.debug_struct("RecordBatch");
        de.field("base_offset", &self.base_offset);
        de.field("last_offset_delta", &self.last_offset_delta);
        //de.field("magic", &self.header.magic());
        //de.field("offset", &(self.header.base_offset()..=self.header.last_offset()));
        //de.field("sequence", &(self.header.base_sequence()..=self.header.last_sequence()));
        //de.field("is_transactional", &self.header.is_transactional());
        //de.field("is_control_batch", &self.header.is_control_batch());
        //de.field("compression_type", &self.header.compression_type());
        //de.field("timestamp_type", &self.header.timestamp_type());
        //de.field("crc", &self.header.checksum());
        de.field("records_count", &self.records_count());
        de.field("records", &self.records());
        de.finish()
    }
}

/// Similar to [i32::wrapping_add], but wrap to `0` instead of [i32::MIN].
pub fn increment_sequence(sequence: i32, increment: i32) -> i32 {
    if sequence > i32::MAX - increment {
        increment - (i32::MAX - sequence) - 1
    } else {
        sequence + increment
    }
}

/// Similar to [i32::wrapping_add], but wrap at `0` instead of [i32::MIN].
pub fn decrement_sequence(sequence: i32, decrement: i32) -> i32 {
    if sequence < decrement {
        i32::MAX - (decrement - sequence) + 1
    } else {
        sequence - decrement
    }
}
//
// impl RecordBatchHeader {
//
//     // pub fn set_partition_leader_epoch(&mut self, epoch: i32) {
//     //     self.buf
//     //         .mut_slice_in(PARTITION_LEADER_EPOCH_OFFSET..)
//     //         .put_i32(epoch);
//     // }
//     //
//     // pub fn magic(&self) -> i8 {
//     //     (&self.buf[MAGIC_OFFSET..]).get_i8()
//     // }
//     //
//     // pub fn base_offset(&self) -> i64 {
//     //     (&self.buf[BASE_OFFSET_OFFSET..]).get_i64()
//     // }
//     //
//
//
//     //
//     // pub fn base_sequence(&self) -> i32 {
//     //     (&self.buf[BASE_SEQUENCE_OFFSET..]).get_i32()
//     // }
//     //
//     // pub fn last_sequence(&self) -> i32 {
//     //     match self.base_sequence() {
//     //         NO_SEQUENCE => NO_SEQUENCE,
//     //         seq => increment_sequence(seq, self.last_offset_delta()),
//     //     }
//     // }
//     //
//     // fn last_offset_delta(&self) -> i32 {
//     //     (&self.buf[LAST_OFFSET_DELTA_OFFSET..]).get_i32()
//     // }
//     //
//     // pub fn max_timestamp(&self) -> i64 {
//     //     (&self.buf[MAX_TIMESTAMP_OFFSET..]).get_i64()
//     // }
//     //
//     // pub fn checksum(&self) -> u32 {
//     //     (&self.buf[CRC_OFFSET..]).get_u32()
//     // }
//     //
//     // pub fn is_transactional(&self) -> bool {
//     //     self.attributes() & TRANSACTIONAL_FLAG_MASK > 0
//     // }
//     //
//     // pub fn is_control_batch(&self) -> bool {
//     //     self.attributes() & CONTROL_FLAG_MASK > 0
//     // }
//     //
//     // pub fn timestamp_type(&self) -> TimestampType {
//     //     if self.attributes() & TIMESTAMP_TYPE_MASK != 0 {
//     //         TimestampType::LogAppendTime
//     //     } else {
//     //         TimestampType::CreateTime
//     //     }
//     // }
//
//     // pub fn compression_type(&self) -> CompressionType {
//     //     (self.attributes() & COMPRESSION_CODEC_MASK).into()
//     // }
//     //
//     // pub fn delete_horizon_ms(&self) -> Option<i64> {
//     //     if self.has_delete_horizon_ms() {
//     //         Some((&self.buf[BASE_TIMESTAMP_OFFSET..]).get_i64())
//     //     } else {
//     //         None
//     //     }
//     // }
//     //
//     // fn has_delete_horizon_ms(&self) -> bool {
//     //     self.attributes() & DELETE_HORIZON_FLAG_MASK > 0
//     // }
//
//     note we're not using the second byte of attributes

// }

impl RecordBatch {
    pub fn new(buf: ByteBuffer, expiration: i64, base_offset: i64, last_offset_delta: i64) -> RecordBatch {
        let mut records = buf.slice(RECORDS_COUNT_OFFSET..);
        let record_size = (&buf[LENGTH_OFFSET..]).get_i32();
        let batch_size = record_size as usize + LOG_OVERHEAD;
        RecordBatch {
            base_offset,
            batch_size,
            last_offset_delta,
            expiration,
            records: RecordList.decode(&mut records).expect("malformed records"),
        }
    }

    pub fn records_count(&self) -> i32 {
        self.records.len() as i32
    }

    pub fn records(&self) -> Vec<Record> {
        self.records.clone()
    }

    pub fn set_last_offset(&mut self, offset: i64) {
        /*
                let base_offset = offset - self.last_offset_delta() as i64;
        self.buf
            .mut_slice_in(BASE_OFFSET_OFFSET..)
            .put_i64(base_offset);
         */
        self.base_offset = offset - self.last_offset_delta;
    }

    pub fn last_offset(&self) -> i64 {
        self.base_offset + self.last_offset_delta
    }

    fn to_crc(&self, data: &[u8]) -> u32 {
        Crc::<u32>::new(&crc::CRC_32_ISCSI).checksum(data)
    }

    pub fn encode(&self, mut buf: &mut BytesMut) {
        /*
        baseOffset: int64
batchLength: int32
partitionLeaderEpoch: int32
magic: int8 (current magic value is 2)
crc: uint32 of everything after it - - V
attributes: int16
    bit 0~2:
        0: no compression
        1: gzip
        2: snappy
        3: lz4
        4: zstd
    bit 3: timestampType
    bit 4: isTransactional (0 means not transactional)
    bit 5: isControlBatch (0 means not a control batch)
    bit 6: hasDeleteHorizonMs (0 means baseTimestamp is not set as the delete horizon for compaction)
    bit 7~15: unused
lastOffsetDelta: int32
baseTimestamp: int64
maxTimestamp: int64
producerId: int64
producerEpoch: int16
baseSequence: int32
records: [Record]
         */
        buf.put_i64(self.base_offset);
        buf.put_i32(0); // length to be filled
        buf.put_i32(0); // partition leader epoch
        buf.put_i8(2); // magic
        buf.put_i32(0); // crc
        buf.put_i16(0); // attributes
        buf.put_i32(self.last_offset_delta as i32);
        buf.put_i64(0); // base timestamp
        buf.put_i64(0); // max timestamp
        buf.put_i64(0); // producer ID
        buf.put_i16(0); // producer epoch
        buf.put_i32(0); // base sequence
        buf.put_i32(self.records_count());
        let mut builder = SendBuilder::new();
        for record in &self.records {
            record.encode(&mut builder, &record).expect("TODO: panic message");
        }
        let bytes = builder.get_bytes();
        let bytes = bytes.as_slice();
        buf.put_slice(bytes);
        buf.put_i8(0); // TODO: tags
        let crc = self.to_crc(&buf[ATTRIBUTES_OFFSET..]);

        let mut crc_buf = &mut buf[CRC_OFFSET..CRC_OFFSET+CRC_LENGTH];
        crc_buf.put_u32(crc);

        let len = buf.len()  - LOG_OVERHEAD;
        let mut size_buf = &mut buf[LENGTH_OFFSET..LENGTH_OFFSET+LENGTH_LENGTH];
        size_buf.put_i32(len as i32);
        // buf[CRC_OFFSET..].put_u32(crc);
        error!("RecordBatch::encode: {:?}", buf);
        error!("Computed CRC: {}", crc);
    }
}

#[cfg(test)]
mod tests {
    use std::io;

    use super::*;
    use crate::records::MutableRecords;

    const RECORD: &[u8] = &[
        0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, // first offset
        0x0, 0x0, 0x0, 0x52, // record batch size
        0xFF, 0xFF, 0xFF, 0xFF, // partition leader epoch
        0x2,  // magic byte
        0xE2, 0x3F, 0xC9, 0x74, // crc
        0x0, 0x0, // attributes
        0x0, 0x0, 0x0, 0x0, // last offset delta
        0x0, 0x0, 0x1, 0x89, 0xAF, 0x78, 0x40, 0x72, // base timestamp
        0x0, 0x0, 0x1, 0x89, 0xAF, 0x78, 0x40, 0x72, // max timestamp
        0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, // producer ID
        0x0, 0x0, // producer epoch
        0x0, 0x0, 0x0, 0x0, // base sequence
        0x0, 0x0, 0x0, 0x1,  // record counts
        0x40, // first record size
        0x0,  // attribute
        0x0,  // timestamp delta
        0x0,  // offset delta
        0x1,  // key length (zigzag : -1)
        // empty key payload
        0x34, // value length (zigzag : 26)
        0x54, 0x68, 0x69, 0x73, 0x20, 0x69, 0x73, 0x20, 0x74, 0x68, 0x65, 0x20, 0x66, 0x69, 0x72,
        0x73, 0x74, 0x20, 0x6D, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x2E, // value payload
        0x0,  // header counts
    ];

    #[test]
    fn test_codec_records() -> io::Result<()> {
        let records = MutableRecords::new(ByteBuffer::new(RECORD.to_vec()));
        let record_batches = records.batches();
        assert_eq!(record_batches.len(), 1);
        let record_batch = &record_batches[0];
        assert_eq!(record_batch.records_count(), 1);
        let record_vec = record_batch.records();
        assert_eq!(record_vec.len(), 1);
        let record = &record_vec[0];
        assert_eq!(record.key_len, -1);
        assert_eq!(record.key, None);
        assert_eq!(record.value_len, 26);
        assert_eq!(
            record.value.as_deref().map(String::from_utf8_lossy),
            Some("This is the first message.".into())
        );
        Ok(())
    }
}
