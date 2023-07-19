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

use bytes::Buf;

use crate::codec::*;

#[derive(Debug, Default)]
pub struct RequestHeader {
    /// The API key of this request.
    pub request_api_key: i16,
    /// The API version of this request.
    pub request_api_version: i16,
    /// The correlation ID of this request.
    pub correlation_id: i32,
    /// The client ID string.
    pub client_id: String,
    /// Unknown tagged fields.
    pub unknown_tagged_fields: Vec<RawTaggedField>,
}

impl Decodable for RequestHeader {
    fn decode<B: Buf>(buf: &mut B, version: i16) -> io::Result<Self> {
        let mut res = RequestHeader {
            request_api_key: Int16.decode(buf)?,
            request_api_version: Int16.decode(buf)?,
            correlation_id: Int32.decode(buf)?,
            ..Default::default()
        };
        if version >= 1 {
            res.client_id = NullableString(false).decode(buf)?.unwrap_or_default();
        }
        if version >= 2 {
            res.unknown_tagged_fields = RawTaggedFieldList.decode(buf)?;
        }
        Ok(res)
    }
}
