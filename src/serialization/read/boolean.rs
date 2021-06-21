// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use parquet_format::Encoding;

use crate::error::Result;
use crate::metadata::ColumnDescriptor;
use crate::read::Page;
use crate::read::PageHeader;

const BIT_MASK: [u8; 8] = [1, 2, 4, 8, 16, 32, 64, 128];

/// Returns whether bit at position `i` in `byte` is set or not
#[inline]
pub fn is_set(byte: u8, i: usize) -> bool {
    (byte & BIT_MASK[i]) != 0
}

/// Returns whether bit at position `i` in `data` is set or not
#[inline]
pub fn get_bit(data: &[u8], i: usize) -> bool {
    // in PLAIN:
    // * Most significant byte is the last one
    // * Most significant bit is the last one
    // note that this is different from Arrow, where most significant byte is the first
    is_set(data[data.len() - 1 - i / 8], i % 8)
}

fn read_bitmap(values: &[u8], length: usize) -> Vec<Option<bool>> {
    (0..length).map(|i| Some(get_bit(values, i))).collect()
}

pub fn page_to_vec(page: &Page, _: &ColumnDescriptor) -> Result<Vec<Option<bool>>> {
    match page.header() {
        PageHeader::V1(_) => match page.encoding() {
            Encoding::Plain | Encoding::PlainDictionary => {
                Ok(read_bitmap(page.buffer(), page.num_values()))
            }
            _ => todo!(),
        },
        PageHeader::V2(_) => match page.encoding() {
            Encoding::Plain | Encoding::PlainDictionary => {
                Ok(read_bitmap(page.buffer(), page.num_values()))
            }
            _ => todo!(),
        },
    }
}
