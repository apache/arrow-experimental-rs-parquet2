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

use super::uleb128;

pub fn decode(values: &[u8]) -> (i64, usize) {
    let (u, consumed) = uleb128::decode(values);
    ((u >> 1) as i64 ^ -((u & 1) as i64), consumed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic() {
        // see e.g. https://stackoverflow.com/a/2211086/931303
        let cases = vec![
            (0u8, 0i64),
            (1, -1),
            (2, 1),
            (3, -2),
            (4, 2),
            (5, -3),
            (6, 3),
            (7, -4),
            (8, 4),
            (9, -5),
        ];
        for (data, expected) in cases {
            let (result, _) = decode(&[data]);
            assert_eq!(result, expected)
        }
    }
}
