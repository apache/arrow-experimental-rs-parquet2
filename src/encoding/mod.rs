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

use std::convert::TryInto;

pub mod bitpacking;
pub mod delta_bitpacked;
pub mod delta_byte_array;
pub mod delta_length_byte_array;
pub mod hybrid_rle;
pub mod plain_byte_array;
pub mod uleb128;
pub mod zigzag_leb128;

pub use parquet_format::Encoding;

/// # Panics
/// This function panics iff `values.len() < 4`.
pub fn get_length(values: &[u8]) -> u32 {
    u32::from_le_bytes(values[0..4].try_into().unwrap())
}

/// Returns floor(log2(x))
#[inline]
pub fn log2(mut x: u64) -> u32 {
    if x == 1 {
        return 1;
    }
    x -= 1;
    let mut result = 0;
    while x > 0 {
        x >>= 1;
        result += 1;
    }
    result
}

/// Returns the ceil of value/divisor
#[inline]
pub fn ceil8(value: usize) -> usize {
    value / 8 + ((value % 8 != 0) as usize)
}
