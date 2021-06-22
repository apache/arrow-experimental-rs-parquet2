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

use parquet_tools::dump::dump_file;
use parquet_tools::meta::show_meta;
use parquet_tools::rows::show_rows;

use parquet_tools::Result;

use clap::{load_yaml, App};

fn main() -> Result<()> {
    let yaml = load_yaml!("cli.yaml");
    let matches = App::from_yaml(yaml).get_matches();

    let file_name = match matches.value_of("file") {
        Some(file_name) => file_name,
        None => {
            eprintln!("No parquet file to read");
            std::process::exit(1);
        }
    };

    let mut output = std::io::stdout();
    if matches.subcommand_matches("rowcount").is_some() {
        show_rows(file_name, &mut output)?;
    }

    if let Some(matches) = matches.subcommand_matches("meta") {
        show_meta(
            file_name,
            matches.is_present("extra"),
            matches.is_present("stats"),
            &mut output,
        )?;
    }

    if let Some(matches) = matches.subcommand_matches("dump") {
        // The column sample size is controlled as an argument to the command line.
        // The default value is 5.
        let sample_size: usize = match matches.value_of("size") {
            Some(val) => val.parse::<usize>().unwrap_or(5),
            None => 5,
        };

        // The columns to be listed can be selected using the columns argument
        // If no argument is chosen then a sample of all the columns if presented
        let columns: Option<Vec<usize>> = match matches.values_of("columns") {
            Some(values) => {
                let columns: std::result::Result<Vec<usize>, _> =
                    values.map(|val| val.parse::<usize>()).collect();
                Some(columns?)
            }
            None => None,
        };

        dump_file(file_name, sample_size, columns, &mut output)?;
    }

    Ok(())
}
