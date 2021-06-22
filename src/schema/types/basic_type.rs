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

use super::Repetition;

/// Common type information.
#[derive(Clone, Debug, PartialEq)]
pub struct BasicTypeInfo {
    name: String,
    // Parquet Spec:
    //   Root of the schema does not have a repetition.
    //   All other types must have one.
    repetition: Repetition,
    is_root: bool,
    id: Option<i32>,
}

// Accessors
impl BasicTypeInfo {
    /// Returns field name.
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn is_root(&self) -> bool {
        self.is_root
    }

    /// Returns [`Repetition`](crate::basic::Repetition) value for the type.
    /// Returns `Optional` if the repetition is not defined
    pub fn repetition(&self) -> &Repetition {
        &self.repetition
    }

    /// Returns `true` if id is set, `false` otherwise.
    pub fn id(&self) -> &Option<i32> {
        &self.id
    }
}

// Constructors
impl BasicTypeInfo {
    pub fn new(name: String, repetition: Repetition, id: Option<i32>, is_root: bool) -> Self {
        Self {
            name,
            repetition,
            is_root,
            id,
        }
    }
}
