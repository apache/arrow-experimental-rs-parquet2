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

mod column_chunk;
mod file;
mod page;
mod row_group;
pub(self) mod statistics;

#[cfg(feature = "stream")]
pub mod stream;

mod dyn_iter;
pub use dyn_iter::DynIter;

pub use file::write_file;
use parquet_format::CompressionCodec;

use crate::read::CompressedPage;

pub type RowGroupIter<'a, E> =
    DynIter<'a, std::result::Result<DynIter<'a, std::result::Result<CompressedPage, E>>, E>>;

#[derive(Debug, Copy, Clone)]
pub struct WriteOptions {
    pub write_statistics: bool,
    pub compression: CompressionCodec,
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;

    use crate::{
        error::Result, metadata::SchemaDescriptor, read::read_metadata,
        serialization::write::primitive::array_to_page_v1,
    };

    #[test]
    fn basic() -> Result<()> {
        let array = vec![
            Some(0),
            Some(1),
            Some(2),
            Some(3),
            Some(4),
            Some(5),
            Some(6),
        ];

        let options = WriteOptions {
            write_statistics: false,
            compression: CompressionCodec::Uncompressed,
        };

        let schema = SchemaDescriptor::try_from_message("message schema { OPTIONAL INT32 col; }")?;

        let row_groups = std::iter::once(Ok(DynIter::new(std::iter::once(Ok(DynIter::new(
            std::iter::once(array_to_page_v1(&array, &options, &schema.columns()[0])),
        ))))));

        let mut writer = Cursor::new(vec![]);
        write_file(&mut writer, row_groups, schema, options, None, None)?;

        let data = writer.into_inner();
        let mut reader = Cursor::new(data);

        let metadata = read_metadata(&mut reader)?;

        // validated against an equivalent array produced by pyarrow.
        let expected = 51;
        assert_eq!(
            metadata.row_groups[0].columns()[0].uncompressed_size(),
            expected
        );

        Ok(())
    }
}
