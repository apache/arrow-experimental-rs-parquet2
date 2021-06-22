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

use parquet2::read::read_metadata;
use std::{fs::File, io::Write, path::Path};

use super::Result;

pub fn show_rows<T, W>(file_name: T, writer: &mut W) -> Result<()>
where
    T: AsRef<Path>,
    W: Write,
{
    let mut file = File::open(file_name)?;

    let metadata = read_metadata(&mut file)?;

    write!(writer, "Total RowCount: {}", metadata.num_rows)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_show_rows() {
        let file_name = "data/sample.parquet";
        let mut buf = Vec::new();

        show_rows(file_name, &mut buf).unwrap();

        let string_output = String::from_utf8(buf).unwrap();
        let expected = "Total RowCount: 100".to_string();
        assert_eq!(expected, string_output);
    }
}
