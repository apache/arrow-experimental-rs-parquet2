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
use crate::{schema::types::PhysicalType, types, types::NativeType};

use super::PageDict;

#[derive(Debug)]
pub struct PrimitivePageDict<T: NativeType> {
    values: Vec<T>,
}

impl<T: NativeType> PrimitivePageDict<T> {
    pub fn new(values: Vec<T>) -> Self {
        Self { values }
    }

    pub fn values(&self) -> &[T] {
        &self.values
    }
}

impl<T: NativeType> PageDict for PrimitivePageDict<T> {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn physical_type(&self) -> &PhysicalType {
        &T::TYPE
    }
}

fn read_plain<T: NativeType>(values: &[u8]) -> Vec<T> {
    // read in plain
    let chunks = values.chunks_exact(std::mem::size_of::<T>());
    assert_eq!(chunks.remainder().len(), 0);
    chunks.map(|chunk| types::decode(chunk)).collect()
}

pub fn read<T: NativeType>(
    buf: &[u8],
    num_values: u32,
    _is_sorted: bool,
) -> Result<Arc<dyn PageDict>> {
    let typed_size = num_values as usize * std::mem::size_of::<T>();
    let values = read_plain::<T>(&buf[..typed_size]);
    Ok(Arc::new(PrimitivePageDict::new(values)))
}
