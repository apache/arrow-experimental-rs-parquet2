use std::{
    error::Error,
    io::{Seek, SeekFrom, Write},
};

use parquet_format::{CompressionCodec, FileMetaData};

use thrift::protocol::TCompactOutputProtocol;
use thrift::protocol::TOutputProtocol;

use crate::{
    error::{ParquetError, Result},
    metadata::SchemaDescriptor,
    read::CompressedPage,
    FOOTER_SIZE, PARQUET_MAGIC,
};

use super::row_group::write_row_group;

fn start_file<W: Write>(writer: &mut W) -> Result<()> {
    Ok(writer.write_all(&PARQUET_MAGIC)?)
}

fn end_file<W: Write + Seek>(mut writer: &mut W, metadata: FileMetaData) -> Result<()> {
    // Write file metadata
    let start_pos = writer.seek(SeekFrom::Current(0))?;
    {
        let mut protocol = TCompactOutputProtocol::new(&mut writer);
        metadata.write_to_out_protocol(&mut protocol)?;
        protocol.flush()?
    }
    let end_pos = writer.seek(SeekFrom::Current(0))?;
    let metadata_len = (end_pos - start_pos) as i32;

    // Write footer
    let metadata_len = metadata_len.to_le_bytes();
    let mut footer_buffer = [0u8; FOOTER_SIZE as usize];
    (0..4).for_each(|i| {
        footer_buffer[i] = metadata_len[i];
    });

    (&mut footer_buffer[4..]).write_all(&PARQUET_MAGIC)?;
    writer.write_all(&footer_buffer)?;
    Ok(())
}

pub fn write_file<
    W,
    I,   // iterator over pages
    II,  // iterator over columns
    III, // iterator over row groups
    E,   // external error any of the iterators may emit
>(
    writer: &mut W,
    schema: SchemaDescriptor,
    codec: CompressionCodec,
    row_groups: III,
) -> Result<()>
where
    W: Write + Seek,
    I: Iterator<Item = std::result::Result<CompressedPage, E>>,
    II: Iterator<Item = std::result::Result<I, E>>,
    III: Iterator<Item = std::result::Result<II, E>>,
    E: Error + Send + Sync + 'static,
{
    start_file(writer)?;

    let row_groups = row_groups
        .map(|row_group| {
            write_row_group(
                writer,
                &schema,
                codec,
                row_group.map_err(ParquetError::from_external_error)?,
            )
        })
        .collect::<Result<Vec<_>>>()?;

    // compute file stats
    let num_rows = row_groups.iter().map(|group| group.num_rows).sum();

    let metadata = FileMetaData::new(
        1,
        schema.into_thrift()?,
        num_rows,
        row_groups,
        None,
        None,
        None,
    );

    end_file(writer, metadata)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{fs::File, io::Cursor};

    use super::*;

    use crate::error::Result;
    use crate::read::read_metadata;
    use crate::tests::get_path;

    #[test]
    fn empty_file() -> Result<()> {
        let mut testdata = get_path();
        testdata.push("alltypes_plain.parquet");
        let mut file = File::open(testdata).unwrap();

        let mut metadata = read_metadata(&mut file)?;

        // take away all groups and rows
        metadata.row_groups = vec![];
        metadata.num_rows = 0;

        let mut writer = Cursor::new(vec![]);

        // write the file
        start_file(&mut writer)?;
        end_file(&mut writer, metadata.into_thrift()?)?;

        let a = writer.into_inner();

        // read it again:
        let result = read_metadata(&mut Cursor::new(a));
        assert!(result.is_ok());

        Ok(())
    }
}