use std::io;

use crate::{
    codec::{
        Decoder, Int32, Int64, Int8, NullableArray, NullableString, RawTaggedFieldList, Struct,
    },
    err_decode_message_null, Deserializable, RawTaggedField, Readable,
};

// https://github.com/aovestdipaperino/kafka-protocol-rs/blob/main/src/messages/list_offsets_request.rs

#[derive(Debug, Default, Clone)]
pub struct ListOffsetsRequest {
    pub replica_id: i32,
    pub isolation_level: i8,
    pub topics: Option<Vec<ListOffsetsRequestTopic>>,
    /// Unknown tagged fields.
    pub unknown_tagged_fields: Vec<RawTaggedField>,
}

#[derive(Debug, Default, Clone)]
pub struct ListOffsetsRequestTopic {
    pub name: String,
    pub partitions: Vec<ListOffsetsRequestPartition>,
    /// Unknown tagged fields.
    pub unknown_tagged_fields: Vec<RawTaggedField>,
}

#[derive(Debug, Default, Clone)]
pub struct ListOffsetsRequestPartition {
    pub partition_index: i32,
    pub current_leader_epoch: i32,
    pub timestamp: i64,
    pub max_num_offsets: i32,
    /// Unknown tagged fields.
    pub unknown_tagged_fields: Vec<RawTaggedField>,
}

impl Deserializable for ListOffsetsRequest {
    fn read<B: Readable>(buf: &mut B, version: i16) -> io::Result<Self> {
        let mut res = ListOffsetsRequest {
            replica_id: Int32.decode(buf)?,
            ..Default::default()
        };
        if version >= 2 {
            res.isolation_level = Int8.decode(buf)?;
        }
        res.topics = NullableArray(Struct(version), version >= 6).decode(buf)?;
        if version >= 6 {
            res.unknown_tagged_fields = RawTaggedFieldList.decode(buf)?;
        }
        Ok(res)
    }
}

impl Deserializable for ListOffsetsRequestTopic {
    fn read<B: Readable>(buf: &mut B, version: i16) -> io::Result<Self> {
        let mut res = ListOffsetsRequestTopic {
            name: NullableString(version >= 6)
                .decode(buf)?
                .ok_or_else(|| err_decode_message_null("name"))?,
            partitions: NullableArray(Struct(version), version >= 6)
                .decode(buf)?
                .ok_or_else(|| err_decode_message_null("partitions"))?,
            ..Default::default()
        };
        if version >= 6 {
            res.unknown_tagged_fields = RawTaggedFieldList.decode(buf)?;
        }
        Ok(res)
    }
}

impl Deserializable for ListOffsetsRequestPartition {
    fn read<B: Readable>(buf: &mut B, version: i16) -> io::Result<Self> {
        let mut res = ListOffsetsRequestPartition {
            partition_index: Int32.decode(buf)?,
            ..Default::default()
        };
        if version == 0 {
            res.max_num_offsets = Int32.decode(buf)?;
        }
        if version >= 4 {
            res.current_leader_epoch = Int32.decode(buf)?;
        }
        res.timestamp = Int64.decode(buf)?;
        if version >= 6 {
            res.unknown_tagged_fields = RawTaggedFieldList.decode(buf)?;
        }
        Ok(res)
    }
}
