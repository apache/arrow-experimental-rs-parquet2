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

use std::io::{Seek, SeekFrom, Write};
use std::sync::Arc;

use parquet_format::{PageHeader as ParquetPageHeader, PageType};
use thrift::protocol::TCompactOutputProtocol;
use thrift::protocol::TOutputProtocol;

use crate::error::Result;
use crate::read::CompressedPage;
use crate::read::PageHeader;
use crate::statistics::Statistics;

/// Contains page write metrics.
pub struct PageWriteSpec {
    pub header: ParquetPageHeader,
    pub header_size: usize,
    pub offset: u64,
    pub bytes_written: u64,
    pub statistics: Option<Arc<dyn Statistics>>,
}

pub fn write_page<W: Write + Seek>(
    writer: &mut W,
    compressed_page: CompressedPage,
) -> Result<PageWriteSpec> {
    let header = assemble_page_header(&compressed_page);

    let start_pos = writer.seek(SeekFrom::Current(0))?;

    let header_size = write_page_header(writer, &header)?;

    writer.write_all(&compressed_page.buffer)?;

    let end_pos = writer.seek(SeekFrom::Current(0))?;

    Ok(PageWriteSpec {
        header,
        header_size,
        offset: start_pos,
        bytes_written: end_pos - start_pos,
        statistics: compressed_page.statistics().transpose()?,
    })
}

fn assemble_page_header(compressed_page: &CompressedPage) -> ParquetPageHeader {
    let mut page_header = ParquetPageHeader {
        type_: match compressed_page.header() {
            PageHeader::V1(_) => PageType::DataPage,
            PageHeader::V2(_) => PageType::DataPageV2,
        },
        uncompressed_page_size: compressed_page.uncompressed_size() as i32,
        compressed_page_size: compressed_page.compressed_size() as i32,
        crc: None,
        data_page_header: None,
        index_page_header: None,
        dictionary_page_header: None,
        data_page_header_v2: None,
    };

    match compressed_page.header() {
        PageHeader::V1(header) => {
            page_header.data_page_header = Some(header.clone());
        }
        PageHeader::V2(header) => {
            page_header.data_page_header_v2 = Some(header.clone());
        }
    }
    page_header
}

/// writes the page header into `writer`, returning the number of bytes used in the process.
fn write_page_header<W: Write + Seek>(
    mut writer: &mut W,
    header: &ParquetPageHeader,
) -> Result<usize> {
    let start_pos = writer.seek(SeekFrom::Current(0))?;
    {
        let mut protocol = TCompactOutputProtocol::new(&mut writer);
        header.write_to_out_protocol(&mut protocol)?;
        protocol.flush()?;
    }
    let end_pos = writer.seek(SeekFrom::Current(0))?;
    Ok((end_pos - start_pos) as usize)
}
