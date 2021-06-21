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

use std::sync::Arc;

use parquet_format::Statistics as ParquetStatistics;

use super::Statistics;
use crate::{
    error::{ParquetError, Result},
    schema::types::PhysicalType,
};

#[derive(Debug, Clone, PartialEq)]
pub struct BooleanStatistics {
    pub null_count: Option<i64>,
    pub distinct_count: Option<i64>,
    pub max_value: Option<bool>,
    pub min_value: Option<bool>,
}

impl Statistics for BooleanStatistics {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn physical_type(&self) -> &PhysicalType {
        &PhysicalType::Boolean
    }
}

pub fn read(v: &ParquetStatistics) -> Result<Arc<dyn Statistics>> {
    if let Some(ref v) = v.max_value {
        if v.len() != std::mem::size_of::<bool>() {
            return Err(ParquetError::OutOfSpec(
                "The max_value of statistics MUST be plain encoded".to_string(),
            ));
        }
    };
    if let Some(ref v) = v.min_value {
        if v.len() != std::mem::size_of::<bool>() {
            return Err(ParquetError::OutOfSpec(
                "The min_value of statistics MUST be plain encoded".to_string(),
            ));
        }
    };

    Ok(Arc::new(BooleanStatistics {
        null_count: v.null_count,
        distinct_count: v.distinct_count,
        max_value: v.max_value.as_ref().and_then(|x| x.get(0)).map(|x| *x != 0),
        min_value: v.min_value.as_ref().and_then(|x| x.get(0)).map(|x| *x != 0),
    }))
}

pub fn write(v: &BooleanStatistics) -> ParquetStatistics {
    ParquetStatistics {
        null_count: v.null_count,
        distinct_count: v.distinct_count,
        max_value: v.max_value.map(|x| vec![x as u8]),
        min_value: v.min_value.map(|x| vec![x as u8]),
        min: None,
        max: None,
    }
}
