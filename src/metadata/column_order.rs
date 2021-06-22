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

use super::sort::SortOrder;

/// Column order that specifies what method was used to aggregate min/max values for
/// statistics.
///
/// If column order is undefined, then it is the legacy behaviour and all values should
/// be compared as signed values/bytes.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ColumnOrder {
    /// Column uses the order defined by its logical or physical type
    /// (if there is no logical type), parquet-format 2.4.0+.
    TypeDefinedOrder(SortOrder),
    /// Undefined column order, means legacy behaviour before parquet-format 2.4.0.
    /// Sort order is always SIGNED.
    Undefined,
}

impl ColumnOrder {
    /// Returns sort order associated with this column order.
    pub fn sort_order(&self) -> SortOrder {
        match *self {
            ColumnOrder::TypeDefinedOrder(order) => order,
            ColumnOrder::Undefined => SortOrder::Signed,
        }
    }
}
