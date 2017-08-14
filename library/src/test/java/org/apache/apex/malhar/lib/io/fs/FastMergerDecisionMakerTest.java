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

import java.io.IOException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.apache.apex.malhar.lib.io.fs.FileStitcher.BlockNotFoundException;
import org.apache.apex.malhar.lib.io.fs.HDFSFileMerger.FastMergerDecisionMaker;
import org.apache.apex.malhar.lib.io.fs.Synchronizer.OutputFileMetadata;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import static org.junit.Assert.fail;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link FastMergerDecisionMaker}
 */

public class FastMergerDecisionMakerTest
{
  private static final long DEFAULT_BLOCK_SIZE = 100L;
  private static final short[] DEFAULT_REPLICATION = new short[] {(short)3, (short)3, (short)3 };

  FastMergerDecisionMaker fastMergerDecisionMaker;
  @Mock
  OutputFileMetadata fileMetadata;
  @Mock
  FileSystem appFS;
  @Mock
  FileStatus status0;
  @Mock
  FileStatus status1;
  @Mock
  FileStatus status2;

  @Before
  public void setup() throws IOException
  {
    MockitoAnnotations.initMocks(this);
    when(appFS.exists(Matchers.any(Path.class))).thenReturn(true);

    when(fileMetadata.getNumberOfBlocks()).thenReturn(3);
    when(fileMetadata.getBlockIds()).thenReturn(new long[] {0L, 1L, 2L });

    fastMergerDecisionMaker = new FastMergerDecisionMaker("", appFS, DEFAULT_BLOCK_SIZE);
  }

  /**
   * If some block is missing then expect BlockNotFoundException.
   *
   * @throws IOException
   * @throws BlockNotFoundException
   */
  @Test(expected = BlockNotFoundException.class)
  public void testMissingBlock() throws IOException, BlockNotFoundException
  {
    when(appFS.exists(new Path("/0"))).thenReturn(true);
    when(appFS.exists(new Path("/1"))).thenReturn(false);

    when(status0.getReplication()).thenReturn(DEFAULT_REPLICATION[0]);
    when(status0.getLen()).thenReturn(DEFAULT_BLOCK_SIZE);

    when(appFS.getFileStatus(new Path("/0"))).thenReturn(status0);

    fastMergerDecisionMaker.isFastMergePossible(fileMetadata);
    fail("Failed when one block missing.");
  }

  private void initializeMocks(long[] blockSizes, short[] replication) throws IOException, BlockNotFoundException
  {
    when(status0.getReplication()).thenReturn(replication[0]);
    when(status1.getReplication()).thenReturn(replication[1]);
    when(status2.getReplication()).thenReturn(replication[2]);

    when(status0.getLen()).thenReturn(blockSizes[0]);
    when(status1.getLen()).thenReturn(blockSizes[1]);
    when(status2.getLen()).thenReturn(blockSizes[2]);

    when(appFS.getFileStatus(new Path("/0"))).thenReturn(status0);
    when(appFS.getFileStatus(new Path("/1"))).thenReturn(status1);
    when(appFS.getFileStatus(new Path("/2"))).thenReturn(status2);
  }

  /**
   * All blocks are of same size which is same as default blockSize. Then fast
   * merge is possible
   *
   * @throws IOException
   * @throws BlockNotFoundException
   */
  @Test
  public void testAllBlockDefaultBlockSize() throws IOException, BlockNotFoundException
  {
    initializeMocks(new long[] {DEFAULT_BLOCK_SIZE, DEFAULT_BLOCK_SIZE, DEFAULT_BLOCK_SIZE }, DEFAULT_REPLICATION);
    Assert.assertTrue(fastMergerDecisionMaker.isFastMergePossible(fileMetadata));
  }

  /**
   * All blocks (except last block)are of same size which is same as default
   * blockSize. Last block is smaller than default blockSize Then fast merge is
   * possible
   *
   * @throws IOException
   * @throws BlockNotFoundException
   */
  @Test
  public void testAllExceptLastBlockDefaultBlockSize() throws IOException, BlockNotFoundException
  {
    initializeMocks(new long[] {DEFAULT_BLOCK_SIZE, DEFAULT_BLOCK_SIZE, DEFAULT_BLOCK_SIZE - 10 },
        DEFAULT_REPLICATION);
    Assert.assertTrue(fastMergerDecisionMaker.isFastMergePossible(fileMetadata));
  }

  /**
   * Some block other than last block is of different size. Then fast merge is
   * not possible
   *
   * @throws IOException
   * @throws BlockNotFoundException
   */
  @Test
  public void testIntermediateBlockDifferentSize() throws IOException, BlockNotFoundException
  {
    initializeMocks(new long[] {DEFAULT_BLOCK_SIZE, DEFAULT_BLOCK_SIZE - 10, DEFAULT_BLOCK_SIZE },
        DEFAULT_REPLICATION);
    Assert.assertFalse(fastMergerDecisionMaker.isFastMergePossible(fileMetadata));
  }

  /**
   * Some block other than last block is of different size. Then fast merge is
   * not possible
   *
   * @throws IOException
   * @throws BlockNotFoundException
   */
  @Test
  public void testAllExceptLastBlockMultipleOfDefaultBlockSize() throws IOException, BlockNotFoundException
  {
    initializeMocks(new long[] {2 * DEFAULT_BLOCK_SIZE, 2 * DEFAULT_BLOCK_SIZE, 2 * DEFAULT_BLOCK_SIZE - 10 },
        DEFAULT_REPLICATION);
    Assert.assertTrue(fastMergerDecisionMaker.isFastMergePossible(fileMetadata));
  }

  /**
   * Some block other than last block is of different size. Then fast merge is
   * not possible
   *
   * @throws IOException
   * @throws BlockNotFoundException
   */
  @Test
  public void testReplicationNotMatching() throws IOException, BlockNotFoundException
  {
    initializeMocks(new long[] {2 * DEFAULT_BLOCK_SIZE, 2 * DEFAULT_BLOCK_SIZE, 2 * DEFAULT_BLOCK_SIZE - 10 },
        new short[] {(short)3, (short)2, (short)3 });
    Assert.assertFalse(fastMergerDecisionMaker.isFastMergePossible(fileMetadata));
  }

}
