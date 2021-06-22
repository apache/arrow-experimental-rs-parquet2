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

use std::{any::Any, sync::Arc};

use crate::error::Result;
use crate::{encoding::get_length, schema::types::PhysicalType};

use super::PageDict;

#[derive(Debug)]
pub struct BinaryPageDict {
    values: Vec<u8>,
    offsets: Vec<i32>,
}

impl BinaryPageDict {
    pub fn new(values: Vec<u8>, offsets: Vec<i32>) -> Self {
        Self { values, offsets }
    }

    pub fn values(&self) -> &[u8] {
        &self.values
    }

    pub fn offsets(&self) -> &[i32] {
        &self.offsets
    }
}

impl PageDict for BinaryPageDict {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn physical_type(&self) -> &PhysicalType {
        &PhysicalType::ByteArray
    }
}

fn read_plain(bytes: &[u8], length: usize) -> (Vec<u8>, Vec<i32>) {
    let mut bytes = bytes;
    let mut values = Vec::new();
    let mut offsets = Vec::with_capacity(length as usize + 1);
    offsets.push(0);

    let mut current_length = 0;
    offsets.extend((0..length).map(|_| {
        let slot_length = get_length(bytes) as i32;
        current_length += slot_length;
        values.extend_from_slice(&bytes[4..4 + slot_length as usize]);
        bytes = &bytes[4 + slot_length as usize..];
        current_length
    }));

    (values, offsets)
}

pub fn read(buf: &[u8], num_values: u32) -> Result<Arc<dyn PageDict>> {
    let (values, offsets) = read_plain(buf, num_values as usize);
    Ok(Arc::new(BinaryPageDict::new(values, offsets)))
}
