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

pub struct DynIter<'a, V> {
    iter: Box<dyn Iterator<Item = V> + 'a>,
}

impl<'iter, V> Iterator for DynIter<'iter, V> {
    type Item = V;
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.iter.size_hint()
    }
}

impl<'iter, V> DynIter<'iter, V> {
    pub fn new<I>(iter: I) -> Self
    where
        I: Iterator<Item = V> + 'iter,
    {
        Self {
            iter: Box::new(iter),
        }
    }
}
