/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.datatorrent.lib.io.fs;

import com.datatorrent.lib.io.block.BlockReader;

/**
 * HDFSInputModule is used to read files/list of files (or directory) from HDFS. <br/>
 * Module emits, <br/>
 * 1. FileMetadata 2. BlockMetadata 3. Block Bytes.<br/><br/>
 * The module reads data in parallel, following parameters can be configured<br/>
 * 1. files: list of file(s)/directories to read<br/>
 * 2. filePatternRegularExp: Files names matching given regex will be read<br/>
 * 3. scanIntervalMillis: interval between two scans to discover new files in input directory<br/>
 * 4. recursive: if scan recursively input directories<br/>
 * 5. blockSize: block size used to read input blocks of file<br/>
 * 6. readersCount: count of readers to read input file<br/>
 * 7. sequencialFileRead: If emit file blocks in sequence?
 */
public class HDFSInputModule extends FSInputModule
{
  @Override
  public FSFileSplitter createFileSplitter()
  {
    return new HDFSFileSplitter();
  }

  @Override
  public BlockReader createBlockReader()
  {
    return new BlockReader();
  }
}
