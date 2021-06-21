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

use super::Type;
use crate::error::{ParquetError, Result};

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum PhysicalType {
    Boolean,
    Int32,
    Int64,
    Int96,
    Float,
    Double,
    ByteArray,
    FixedLenByteArray(i32),
}

pub fn type_to_physical_type(type_: &Type, length: Option<i32>) -> Result<PhysicalType> {
    Ok(match type_ {
        Type::Boolean => PhysicalType::Boolean,
        Type::Int32 => PhysicalType::Int32,
        Type::Int64 => PhysicalType::Int64,
        Type::Int96 => PhysicalType::Int96,
        Type::Float => PhysicalType::Float,
        Type::Double => PhysicalType::Double,
        Type::ByteArray => PhysicalType::ByteArray,
        Type::FixedLenByteArray => {
            let length = length
                .ok_or_else(|| general_err!("Length must be defined for FixedLenByteArray"))?;
            PhysicalType::FixedLenByteArray(length)
        }
    })
}

pub fn physical_type_to_type(physical_type: &PhysicalType) -> (Type, Option<i32>) {
    match physical_type {
        PhysicalType::Boolean => (Type::Boolean, None),
        PhysicalType::Int32 => (Type::Int32, None),
        PhysicalType::Int64 => (Type::Int64, None),
        PhysicalType::Int96 => (Type::Int96, None),
        PhysicalType::Float => (Type::Float, None),
        PhysicalType::Double => (Type::Double, None),
        PhysicalType::ByteArray => (Type::ByteArray, None),
        PhysicalType::FixedLenByteArray(length) => (Type::FixedLenByteArray, Some(*length)),
    }
}
