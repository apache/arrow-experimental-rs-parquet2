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

use std::{
    error::Error,
    io::{Seek, Write},
};

use parquet_format::{CompressionCodec, RowGroup};

use crate::{
    error::{ParquetError, Result},
    metadata::ColumnDescriptor,
    read::CompressedPage,
};

use super::{column_chunk::write_column_chunk, DynIter};

fn same_elements<T: PartialEq + Copy>(arr: &[T]) -> Option<Option<T>> {
    if arr.is_empty() {
        return Some(None);
    }
    let first = &arr[0];
    if arr.iter().all(|item| item == first) {
        Some(Some(*first))
    } else {
        None
    }
}

pub fn write_row_group<
    W,
    E, // external error any of the iterators may emit
>(
    writer: &mut W,
    descriptors: &[ColumnDescriptor],
    codec: CompressionCodec,
    columns: DynIter<std::result::Result<DynIter<std::result::Result<CompressedPage, E>>, E>>,
) -> Result<RowGroup>
where
    W: Write + Seek,
    E: Error + Send + Sync + 'static,
{
    let column_iter = descriptors.iter().zip(columns);

    let columns = column_iter
        .map(|(descriptor, page_iter)| {
            write_column_chunk(
                writer,
                descriptor,
                codec,
                page_iter.map_err(ParquetError::from_external_error)?,
            )
        })
        .collect::<Result<Vec<_>>>()?;

    // compute row group stats
    let num_rows = columns
        .iter()
        .map(|c| c.meta_data.as_ref().unwrap().num_values)
        .collect::<Vec<_>>();
    let num_rows = match same_elements(&num_rows) {
        None => return Err(general_err!("Every column chunk in a row group MUST have the same number of rows. The columns have rows: {:?}", num_rows)),
        Some(None) => 0,
        Some(Some(v)) => v
    };

    let total_byte_size = columns
        .iter()
        .map(|c| c.meta_data.as_ref().unwrap().total_compressed_size)
        .sum();

    Ok(RowGroup {
        columns,
        total_byte_size,
        num_rows,
        sorting_columns: None,
    })
}
