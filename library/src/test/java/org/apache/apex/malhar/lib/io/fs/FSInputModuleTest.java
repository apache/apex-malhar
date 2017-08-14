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

package org.apache.apex.malhar.lib.io.fs;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.apache.apex.malhar.lib.io.block.BlockMetadata;
import org.apache.apex.malhar.lib.io.fs.FSInputModule.SequentialFileBlockMetadataCodec;

import static org.mockito.Mockito.when;

public class FSInputModuleTest
{
  @Mock
  BlockMetadata.FileBlockMetadata file1block1;
  @Mock
  BlockMetadata.FileBlockMetadata file1block2;
  @Mock
  BlockMetadata.FileBlockMetadata file2block1;

  @Test
  public void testSequentialFileBlockMetadataCodec()
  {

    MockitoAnnotations.initMocks(this);
    when(file1block1.getFilePath()).thenReturn("file1");
    when(file1block2.getFilePath()).thenReturn("file1");
    when(file2block1.getFilePath()).thenReturn("file2");

    SequentialFileBlockMetadataCodec codec = new SequentialFileBlockMetadataCodec();

    Assert.assertEquals("Blocks of same file distributed to different partitions", codec.getPartition(file1block1),
        codec.getPartition(file1block2));
    Assert.assertNotSame("Blocks of different files distributed to same partition", codec.getPartition(file1block1),
        codec.getPartition(file2block1));
  }
}
