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

use std::io;

use bytes::BufMut;

use crate::{codec::*, err_encode_message_unsupported};

// Version 1 adds throttle time.
//
// Version 2 and 3 are the same as version 1.
//
// Version 4 adds features for transactional consumption.
//
// Version 5 adds LogStartOffset to indicate the earliest available offset of
// partition data that can be consumed.
//
// Starting in version 6, we may return KAFKA_STORAGE_ERROR as an error code.
//
// Version 7 adds incremental fetch request support.
//
// Starting in version 8, on quota violation, brokers send out responses before throttling.
//
// Version 9 is the same as version 8.
//
// Version 10 indicates that the response data can use the ZStd compression
// algorithm, as described in KIP-110.
// Version 12 adds support for flexible versions, epoch detection through the `TruncationOffset`
// field, and leader discovery through the `CurrentLeader` field
//
// Version 13 replaces the topic name field with topic ID (KIP-516).
//
// Version 14 is the same as version 13 but it also receives a new error called
// OffsetMovedToTieredStorageException (KIP-405)
//
// Version 15 is the same as version 14 (KIP-903).

#[derive(Debug, Default, Clone)]
pub struct FetchResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota violation,
    /// or zero if the request did not violate any quota.
    pub throttle_time_ms: i32,
    /// The top level response error code.
    pub error_code: i16,
    /// The fetch session ID, or 0 if this is not part of a fetch session.
    pub session_id: i32,
    /// The response topics.
    pub responses: Vec<FetchableTopicResponse>,
    /// Unknown tagged fields.
    pub unknown_tagged_fields: Vec<RawTaggedField>,
}

impl Encodable for FetchResponse {
    fn encode<B: BufMut>(&self, buf: &mut B, version: i16) -> io::Result<()> {
        if version >= 1 {
            Int32.encode(buf, self.throttle_time_ms)?;
        }
        if version >= 7 {
            Int16.encode(buf, self.error_code)?;
            Int32.encode(buf, self.session_id)?;
        }
        NullableArray(Struct(version), version >= 12).encode(buf, self.responses.as_slice())?;
        if version >= 12 {
            RawTaggedFieldList.encode(buf, self.unknown_tagged_fields.as_slice())?;
        }
        Ok(())
    }
}

#[derive(Debug, Default, Clone)]
pub struct FetchableTopicResponse {
    /// The topic name.
    pub topic: String,
    /// The unique topic ID
    pub topic_id: uuid::Uuid,
    /// The topic partitions.
    pub partitions: Vec<PartitionData>,
    /// Unknown tagged fields.
    pub unknown_tagged_fields: Vec<RawTaggedField>,
}

impl Encodable for FetchableTopicResponse {
    fn encode<B: BufMut>(&self, buf: &mut B, version: i16) -> io::Result<()> {
        if version <= 12 {
            NullableString(version >= 12).encode(buf, self.topic.as_str())?;
        }
        if version >= 13 {
            Uuid.encode(buf, self.topic_id)?
        }
        NullableArray(Struct(version), version >= 12).encode(buf, self.partitions.as_slice())?;
        if version >= 12 {
            RawTaggedFieldList.encode(buf, self.unknown_tagged_fields.as_slice())?;
        }
        Ok(())
    }
}

#[derive(Debug, Default, Clone)]
pub struct PartitionData {
    /// The topic name.
    pub partition_index: i32,
    /// The error code, or 0 if there was no fetch error.
    pub error_code: i16,
    /// The current high water mark.
    pub high_watermark: i64,
    /// The last stable offset (or LSO) of the partition. This is the last offset such that the
    /// state of all transactional records prior to this offset have been decided (ABORTED or
    /// COMMITTED).
    pub last_stable_offset: i64,
    /// The current log start offset.
    pub log_start_offset: i64,
    /// In case divergence is detected based on the `LastFetchedEpoch` and `FetchOffset` in the
    /// request, this field indicates the largest epoch and its end offset such that subsequent
    /// records are known to diverge
    pub diverging_epoch: Option<EpochEndOffset>,
    pub current_leader: Option<LeaderIdAndEpoch>,
    /// In the case of fetching an offset less than the LogStartOffset, this is the end offset and
    /// epoch that should be used in the FetchSnapshot request.
    pub snapshot_id: Option<SnapshotId>,
    /// The aborted transactions.
    pub aborted_transactions: Option<Vec<AbortedTransaction>>,
    /// The preferred read replica for the consumer to use on its next fetch request
    pub preferred_read_replica: i32,
    /// The record data.
    pub records: bytes::Bytes,
    /// Unknown tagged fields.
    pub unknown_tagged_fields: Vec<RawTaggedField>,
}

impl Encodable for PartitionData {
    fn encode<B: BufMut>(&self, buf: &mut B, version: i16) -> io::Result<()> {
        Int32.encode(buf, self.partition_index)?;
        Int16.encode(buf, self.error_code)?;
        Int64.encode(buf, self.high_watermark)?;
        if version >= 4 {
            Int64.encode(buf, self.last_stable_offset)?;
        }
        if version >= 5 {
            Int64.encode(buf, self.log_start_offset)?
        }
        if version >= 4 {
            NullableArray(Struct(version), version >= 12)
                .encode(buf, self.aborted_transactions.as_deref())?;
        }
        if version >= 11 {
            Int32.encode(buf, self.preferred_read_replica)?;
        }
        NullableBytes(version >= 12).encode(buf, &self.records)?;
        if version >= 12 {
            let mut unknown_tagged_fields = vec![];
            if let Some(diverging_epoch) = &self.diverging_epoch {
                unknown_tagged_fields.push(RawTaggedField {
                    tag: 0,
                    data: Struct(version).encode_alloc(diverging_epoch)?,
                })
            }
            if let Some(current_leader) = &self.current_leader {
                unknown_tagged_fields.push(RawTaggedField {
                    tag: 1,
                    data: Struct(version).encode_alloc(current_leader)?,
                })
            }
            if let Some(snapshot_id) = &self.snapshot_id {
                unknown_tagged_fields.push(RawTaggedField {
                    tag: 2,
                    data: Struct(version).encode_alloc(snapshot_id)?,
                })
            }
            unknown_tagged_fields.append(&mut self.unknown_tagged_fields.clone());
            RawTaggedFieldList.encode(buf, &unknown_tagged_fields)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct EpochEndOffset {
    pub epoch: i32,
    pub end_offset: i64,
    /// Unknown tagged fields.
    pub unknown_tagged_fields: Vec<RawTaggedField>,
}

impl Default for EpochEndOffset {
    fn default() -> Self {
        EpochEndOffset {
            epoch: -1,
            end_offset: -1,
            unknown_tagged_fields: vec![],
        }
    }
}

impl Encodable for EpochEndOffset {
    fn encode<B: BufMut>(&self, buf: &mut B, version: i16) -> io::Result<()> {
        if version < 12 {
            Err(err_encode_message_unsupported(version, "EpochEndOffset"))?
        }
        Int32.encode(buf, self.epoch)?;
        Int64.encode(buf, self.end_offset)?;
        RawTaggedFieldList.encode(buf, self.unknown_tagged_fields.as_slice())?;
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct LeaderIdAndEpoch {
    /// The ID of the current leader or -1 if the leader is unknown.
    pub leader_id: i32,
    /// The latest known leader epoch
    pub leader_epoch: i32,
    /// Unknown tagged fields.
    pub unknown_tagged_fields: Vec<RawTaggedField>,
}

impl Default for LeaderIdAndEpoch {
    fn default() -> Self {
        LeaderIdAndEpoch {
            leader_id: -1,
            leader_epoch: -1,
            unknown_tagged_fields: vec![],
        }
    }
}

impl Encodable for LeaderIdAndEpoch {
    fn encode<B: BufMut>(&self, buf: &mut B, version: i16) -> io::Result<()> {
        if version < 12 {
            Err(err_encode_message_unsupported(version, "LeaderIdAndEpoch"))?
        }
        Int32.encode(buf, self.leader_id)?;
        Int32.encode(buf, self.leader_epoch)?;
        RawTaggedFieldList.encode(buf, self.unknown_tagged_fields.as_slice())?;
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct SnapshotId {
    pub end_offset: i64,
    pub epoch: i32,
    /// Unknown tagged fields.
    pub unknown_tagged_fields: Vec<RawTaggedField>,
}

impl Default for SnapshotId {
    fn default() -> Self {
        SnapshotId {
            end_offset: -1,
            epoch: -1,
            unknown_tagged_fields: vec![],
        }
    }
}

impl Encodable for SnapshotId {
    fn encode<B: BufMut>(&self, buf: &mut B, version: i16) -> io::Result<()> {
        if version < 12 {
            Err(err_encode_message_unsupported(version, "SnapshotId"))?
        }
        Int64.encode(buf, self.end_offset)?;
        Int32.encode(buf, self.epoch)?;
        RawTaggedFieldList.encode(buf, self.unknown_tagged_fields.as_slice())?;
        Ok(())
    }
}

#[derive(Debug, Default, Clone)]
pub struct AbortedTransaction {
    /// The producer id associated with the aborted transaction.
    pub producer_id: i64,
    /// The first offset in the aborted transaction.
    pub first_offset: i64,
    /// Unknown tagged fields.
    pub unknown_tagged_fields: Vec<RawTaggedField>,
}

impl Encodable for AbortedTransaction {
    fn encode<B: BufMut>(&self, buf: &mut B, version: i16) -> io::Result<()> {
        if version < 4 {
            Err(err_encode_message_unsupported(
                version,
                "AbortedTransaction",
            ))?
        }
        Int64.encode(buf, self.producer_id)?;
        Int64.encode(buf, self.first_offset)?;
        if version >= 12 {
            RawTaggedFieldList.encode(buf, self.unknown_tagged_fields.as_slice())?;
        }
        Ok(())
    }
}
