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

use futures::stream::Stream;
use futures::StreamExt;
use futures::TryStreamExt;

use std::{
    error::Error,
    io::{Seek, Write},
};

use parquet_format::FileMetaData;

pub use crate::metadata::KeyValue;
use crate::{
    error::{ParquetError, Result},
    metadata::SchemaDescriptor,
};

use super::file::{end_file, start_file};
use super::{row_group::write_row_group, RowGroupIter, WriteOptions};

pub async fn write_stream<'a, W, S, E>(
    writer: &mut W,
    row_groups: S,
    schema: SchemaDescriptor,
    options: WriteOptions,
    created_by: Option<String>,
    key_value_metadata: Option<Vec<KeyValue>>,
) -> Result<()>
where
    W: Write + Seek,
    S: Stream<Item = std::result::Result<RowGroupIter<'a, E>, E>>,
    E: Error + Send + Sync + 'static,
{
    start_file(writer)?;

    let row_groups = row_groups
        .map(|row_group| {
            write_row_group(
                writer,
                schema.columns(),
                options.compression,
                row_group.map_err(ParquetError::from_external_error)?,
            )
        })
        .try_collect::<Vec<_>>()
        .await?;

    // compute file stats
    let num_rows = row_groups.iter().map(|group| group.num_rows).sum();

    let metadata = FileMetaData::new(
        1,
        schema.into_thrift()?,
        num_rows,
        row_groups,
        key_value_metadata,
        created_by,
        None,
    );

    end_file(writer, metadata)?;
    Ok(())
}
