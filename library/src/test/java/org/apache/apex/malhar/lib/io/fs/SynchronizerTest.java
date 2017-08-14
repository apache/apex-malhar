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

import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import org.apache.apex.malhar.lib.io.block.BlockMetadata.FileBlockMetadata;
import org.apache.apex.malhar.lib.io.fs.AbstractFileSplitter.FileMetadata;
import org.apache.apex.malhar.lib.io.fs.Synchronizer.OutputFileMetadata;
import org.apache.apex.malhar.lib.testbench.CollectorTestSink;

import com.google.common.collect.Lists;

/**
 * Unit tests for {@link Synchronizer}
 */
public class SynchronizerTest
{
  public static final String[] FILE_NAMES = {"a.txt", "b.txt", "c.txt", "d.txt", "e.txt" };

  public static final long[][] BLOCK_IDS = {
      //Block ids for file1 (a.txt)
      {1001, 1002, 1003 },
      //Block ids for file2 (b.txt)
      {1004, 1005, 1006, 1007 },
      //c.txt
      {1008, 1009, 1010 },
      //d.txt
      {1011, 1012 },
      //e.txt
      {1013, 1014 } };

  List<FileMetadata> fileMetadataList;

  List<FileBlockMetadata> blockMetadataList;

  Synchronizer underTest;

  public SynchronizerTest()
  {

    underTest = new Synchronizer();
    fileMetadataList = Lists.newArrayList();
    blockMetadataList = Lists.newArrayList();

    for (int i = 0; i < FILE_NAMES.length; i++) {
      FileMetadata fileMetadata = new FileMetadata(FILE_NAMES[i]);
      fileMetadata.setFileName(FILE_NAMES[i]);
      fileMetadata.setBlockIds(BLOCK_IDS[i]);
      fileMetadata.setNumberOfBlocks(BLOCK_IDS[i].length);

      for (int blockIndex = 0; blockIndex < BLOCK_IDS[i].length; blockIndex++) {
        FileBlockMetadata fileBlockMetadata = new FileBlockMetadata(FILE_NAMES[i]);
        fileBlockMetadata.setBlockId(BLOCK_IDS[i][blockIndex]);
        blockMetadataList.add(fileBlockMetadata);
      }

      fileMetadataList.add(fileMetadata);
    }
  }

  @Test
  public void testSynchronizer()
  {

    CollectorTestSink<OutputFileMetadata> sink = new CollectorTestSink<OutputFileMetadata>();
    underTest.trigger.setSink((CollectorTestSink)sink);

    underTest.filesMetadataInput.process(fileMetadataList.get(0));
    Assert.assertEquals(0, sink.collectedTuples.size());
    underTest.blocksMetadataInput.process(blockMetadataList.get(0));
    underTest.blocksMetadataInput.process(blockMetadataList.get(1));
    Assert.assertEquals(0, sink.collectedTuples.size());
    underTest.blocksMetadataInput.process(blockMetadataList.get(2));
    Assert.assertEquals(1, sink.collectedTuples.size());
    Assert.assertEquals("a.txt", sink.collectedTuples.get(0).getFileName());

    underTest.blocksMetadataInput.process(blockMetadataList.get(3));
    underTest.blocksMetadataInput.process(blockMetadataList.get(4));
    Assert.assertEquals(1, sink.collectedTuples.size());

    underTest.filesMetadataInput.process(fileMetadataList.get(1));
    Assert.assertEquals(1, sink.collectedTuples.size());

    underTest.blocksMetadataInput.process(blockMetadataList.get(5));
    Assert.assertEquals(1, sink.collectedTuples.size());
    underTest.blocksMetadataInput.process(blockMetadataList.get(6));
    Assert.assertEquals(2, sink.collectedTuples.size());
    Assert.assertEquals("b.txt", sink.collectedTuples.get(1).getFileName());

    underTest.blocksMetadataInput.process(blockMetadataList.get(7));
    underTest.blocksMetadataInput.process(blockMetadataList.get(8));
    Assert.assertEquals(2, sink.collectedTuples.size());
    underTest.blocksMetadataInput.process(blockMetadataList.get(9));
    Assert.assertEquals(2, sink.collectedTuples.size());

    underTest.filesMetadataInput.process(fileMetadataList.get(2));
    Assert.assertEquals(3, sink.collectedTuples.size());
    Assert.assertEquals("c.txt", sink.collectedTuples.get(2).getFileName());

    underTest.filesMetadataInput.process(fileMetadataList.get(3));
    underTest.filesMetadataInput.process(fileMetadataList.get(4));
    Assert.assertEquals(3, sink.collectedTuples.size());

    underTest.blocksMetadataInput.process(blockMetadataList.get(10));
    Assert.assertEquals(3, sink.collectedTuples.size());

    underTest.blocksMetadataInput.process(blockMetadataList.get(11));

    Assert.assertEquals(4, sink.collectedTuples.size());
    Assert.assertEquals("d.txt", sink.collectedTuples.get(3).getFileName());

    underTest.blocksMetadataInput.process(blockMetadataList.get(12));
    Assert.assertEquals(4, sink.collectedTuples.size());

    underTest.blocksMetadataInput.process(blockMetadataList.get(13));
    Assert.assertEquals(5, sink.collectedTuples.size());
    Assert.assertEquals("e.txt", sink.collectedTuples.get(4).getFileName());

  }

}
