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
use bytes::BufMut;
use tracing::warn;

use crate::{bytebuffer::ByteBuffer, records::*};
use crate::records::{ByteBuffer as BB, ByteBufferRecords};

#[derive(Default)]
pub struct MutableBatches {
    buf: ByteBuffer,
    batches: OnceCell<Vec<RecordBatch>>,
}

impl Clone for MutableBatches {
    /// ATTENTION - Cloning Records is a heavy operation.
    ///
    /// MutableRecords is a public struct and it has a [MutableBatches::mut_batches] method that
    /// modifies the underlying [ByteBuffer]. If we only do a shallow clone, then two MutableRecords
    /// that doesn't have any ownership overlapping can modify the same underlying bytes.
    ///
    /// Generally, MutableRecords users iterate over batches with [MutableBatches::batches] or
    /// [MutableBatches::mut_batches], and pass ownership instead of clone. This clone behavior is
    /// similar to clone a [Vec].
    ///
    /// To produce a read-only view without copy, use [MutableBatches::freeze] instead.
    fn clone(&self) -> Self {
        warn!("Cloning mutable records will copy bytes and is not encouraged; try MutableRecords::freeze.");
        MutableBatches {
            buf: ByteBuffer::new(self.buf.to_vec()),
            batches: OnceCell::new(),
        }
    }
}

impl Debug for MutableBatches {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(self.batches(), f)
    }
}

impl MutableBatches {
    pub fn new(buf: ByteBuffer) -> Self {
        let batches = OnceCell::new();
        MutableBatches { buf, batches }
    }

    pub fn freeze(self) -> ReadOnlyBatches {
        // Create a new buffer here.
        let mut buf = bytes::BytesMut::new();
        let records = self.batches.get().expect("Should be set");
        //buf.put_i32(records.len() as i32);

        for record_batch in records {
            record_batch.encode(& mut buf);
        }

        ReadOnlyBatches::ByteBuffer(ByteBufferRecords::new(BB::new(buf.to_vec())))
    }

    pub fn as_bytes(&self) -> &[u8] {
        self.buf.as_bytes()
    }

    pub fn mut_batches(&mut self) -> &mut [RecordBatch] {
        self.batches.get_or_init(|| load_batches(&self.buf));
        // SAFETY - init above
        unsafe { self.batches.get_mut().unwrap_unchecked() }
    }

    pub fn batches(&self) -> &[RecordBatch] {
        self.batches.get_or_init(|| load_batches(&self.buf))
    }
}
