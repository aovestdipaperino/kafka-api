use std::io;

use crate::{
    codec::{
        Encoder, FixedSizeEncoder, Int16, Int32, Int64, NullableArray, NullableString,
        RawTaggedFieldList, Struct,
    },
    RawTaggedField, Serializable, Writable,
};

#[derive(Debug, Clone, Default)]
pub struct ListOffsetsResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota violation,
    /// or zero if the request did not violate any quota.
    ///
    /// Supported API versions: 2-8
    pub throttle_time_ms: i32,
    pub topics: Vec<ListOffsetsTopicResponse>,
    /// Other tagged fields
    pub unknown_tagged_fields: Vec<RawTaggedField>,
}

#[derive(Debug, Default, Clone)]
pub struct ListOffsetsTopicResponse {
    pub name: Option<String>,
    pub partitions: Vec<ListOffsetsPartitionResponse>,
    pub unknown_tagged_fields: Vec<RawTaggedField>,
}

impl Serializable for ListOffsetsTopicResponse {
    fn write<B: Writable>(&self, buf: &mut B, version: i16) -> io::Result<()> {
        NullableString(version >= 6).encode(buf, self.name.as_deref())?;
        NullableArray(Struct(version), version >= 6).encode(buf, self.partitions.as_slice())?;
        if version >= 6 {
            RawTaggedFieldList.encode(buf, &self.unknown_tagged_fields)?;
        }
        Ok(())
    }

    fn calculate_size(&self, version: i16) -> usize {
        let mut res = NullableString(version >= 6).calculate_size(self.name.as_deref());
        res +=
            NullableArray(Struct(version), version >= 6).calculate_size(self.partitions.as_slice());
        if version >= 6 {
            res += RawTaggedFieldList.calculate_size(&self.unknown_tagged_fields);
        }
        res
    }
}

#[derive(Debug, Default, Clone)]
pub struct ListOffsetsPartitionResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota violation,
    /// or zero if the request did not violate any quota.
    pub partition_index: i32,
    /// The error code, or 0 if there was no error.
    pub error_code: i16,

    pub old_style_offsets: Vec<i64>,
    pub timestamp: i64,
    pub offset: i64,
    pub leader_epoch: i32,
    pub unknown_tagged_fields: Vec<RawTaggedField>,
}

impl Serializable for ListOffsetsPartitionResponse {
    fn write<B: Writable>(&self, buf: &mut B, version: i16) -> io::Result<()> {
        Int32.encode(buf, self.partition_index)?;
        Int16.encode(buf, self.error_code)?;
        if version == 0 {
            todo!();
        }
        if version >= 1 {
            Int64.encode(buf, self.timestamp)?;
            Int64.encode(buf, self.offset)?;
            Int32.encode(buf, self.leader_epoch)?;
        }
        if version >= 6 {
            RawTaggedFieldList.encode(buf, &self.unknown_tagged_fields)?;
        }
        Ok(())
    }

    fn calculate_size(&self, version: i16) -> usize {
        let mut res = 0;
        res += Int32::SIZE; // self.partition_index
        res += Int16::SIZE; // self.error_code
        if version == 0 {
            todo!();
        }
        if version >= 1 {
            res += Int64::SIZE; // self.timestamp
            res += Int64::SIZE; // self.offset
            res += Int32::SIZE; // self.leader_epoch
        }
        if version >= 6 {
            res += RawTaggedFieldList.calculate_size(&self.unknown_tagged_fields);
        }
        res
    }
}

impl Serializable for ListOffsetsResponse {
    fn write<B: Writable>(&self, buf: &mut B, version: i16) -> io::Result<()> {
        if version >= 2 {
            Int32.encode(buf, self.throttle_time_ms)?;
        }
        NullableArray(Struct(version), version >= 6).encode(buf, self.topics.as_slice())?;
        if version >= 6 {
            RawTaggedFieldList.encode(buf, &self.unknown_tagged_fields)?;
        }
        Ok(())
    }

    fn calculate_size(&self, version: i16) -> usize {
        let mut res = 0;
        if version >= 2 {
            res += Int32::SIZE; // self.throttle_time_ms
        }
        res += NullableArray(Struct(version), version >= 6).calculate_size(self.topics.as_slice());
        if version >= 6 {
            res += RawTaggedFieldList.calculate_size(&self.unknown_tagged_fields);
        }
        res
    }
}
