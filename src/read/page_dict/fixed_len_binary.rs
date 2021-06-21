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
use crate::schema::types::PhysicalType;

use super::PageDict;

#[derive(Debug)]
pub struct FixedLenByteArrayPageDict {
    values: Vec<u8>,
    physical_type: PhysicalType,
    size: usize,
}

impl FixedLenByteArrayPageDict {
    pub fn new(values: Vec<u8>, physical_type: PhysicalType, size: usize) -> Self {
        Self {
            values,
            physical_type,
            size,
        }
    }

    pub fn values(&self) -> &[u8] {
        &self.values
    }

    pub fn size(&self) -> usize {
        self.size
    }
}

impl PageDict for FixedLenByteArrayPageDict {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn physical_type(&self) -> &PhysicalType {
        &self.physical_type
    }
}

fn read_plain(bytes: &[u8], size: usize, length: usize) -> Vec<u8> {
    bytes[..size * length].to_vec()
}

pub fn read(buf: &[u8], size: i32, num_values: u32) -> Result<Arc<dyn PageDict>> {
    let values = read_plain(buf, size as usize, num_values as usize);
    Ok(Arc::new(FixedLenByteArrayPageDict::new(
        values,
        PhysicalType::FixedLenByteArray(size),
        size as usize,
    )))
}
