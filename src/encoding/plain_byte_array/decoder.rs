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

use crate::encoding::get_length;

/// Decodes according to [Plain strings](https://github.com/apache/parquet-format/blob/master/Encodings.md#plain-plain--0),
/// prefixes, lengths and values
/// # Implementation
/// This struct does not allocate on the heap.
#[derive(Debug)]
pub struct Decoder<'a> {
    values: &'a [u8],
    remaining: usize,
}

impl<'a> Decoder<'a> {
    #[inline]
    pub fn new(values: &'a [u8], length: usize) -> Self {
        Self {
            values,
            remaining: length,
        }
    }
}

impl<'a> Iterator for Decoder<'a> {
    type Item = &'a [u8];

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        let values = self.values;
        if values.len() >= 4 {
            let next_len = get_length(values) as usize;
            let values = &values[4..];

            let result = Some(&values[0..next_len]);
            self.values = &values[next_len..];
            self.remaining -= 1;

            result
        } else {
            None
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}
