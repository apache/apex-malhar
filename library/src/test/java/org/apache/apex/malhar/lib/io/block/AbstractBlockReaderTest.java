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
package org.apache.apex.malhar.lib.io.block;

import java.util.Collection;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import org.apache.apex.malhar.lib.counters.BasicCounters;
import org.apache.apex.malhar.lib.util.TestUtils;
import org.apache.commons.lang.mutable.MutableLong;
import org.apache.hadoop.fs.FSDataInputStream;

import com.google.common.collect.Lists;

import com.datatorrent.api.DefaultPartition;
import com.datatorrent.api.Partitioner;
import com.datatorrent.api.Stats;
import com.datatorrent.api.StatsListener;
import com.datatorrent.netlet.util.Slice;

/**
 * Stats and partitioning tests for {@link AbstractBlockReader}
 */
public class AbstractBlockReaderTest
{

  @Test
  public void testAdjustedCount()
  {
    TestReader sliceReader = new TestReader();
    Assert.assertEquals("min", 1, sliceReader.getAdjustedCount(1));
    Assert.assertEquals("max", 16, sliceReader.getAdjustedCount(16));
    Assert.assertEquals("max-1", 8, sliceReader.getAdjustedCount(15));
    Assert.assertEquals("min+1", 2, sliceReader.getAdjustedCount(2));
    Assert.assertEquals("between 1", 4, sliceReader.getAdjustedCount(4));
    Assert.assertEquals("between 2", 4, sliceReader.getAdjustedCount(7));
    Assert.assertEquals("between 2", 8, sliceReader.getAdjustedCount(12));
  }

  @Test
  public void testProcessStatsForPartitionCount()
  {

    TestUtils.MockBatchedOperatorStats readerStats = new TestUtils.MockBatchedOperatorStats(2);
    readerStats.operatorStats = Lists.newArrayList();
    readerStats.operatorStats.add(new ReaderStats(10, 1, 100, 1));

    TestReader sliceReader = new TestReader();

    StatsListener.Response response = sliceReader.processStats(readerStats);

    Assert.assertTrue("partition needed", response.repartitionRequired);
    Assert.assertEquals("partition count changed", 8, sliceReader.getPartitionCount());
  }

  @Test
  public void testProcessStatsForRepeatedPartitionCount() throws InterruptedException
  {

    TestUtils.MockBatchedOperatorStats readerStats = new TestUtils.MockBatchedOperatorStats(2);
    readerStats.operatorStats = Lists.newArrayList();
    readerStats.operatorStats.add(new ReaderStats(10, 1, 100, 1));

    TestReader sliceReader = new TestReader();
    sliceReader.setIntervalMillis(500);

    sliceReader.processStats(readerStats);
    Thread.sleep(500);
    StatsListener.Response response = sliceReader.processStats(readerStats);

    Assert.assertFalse("partition needed", response.repartitionRequired);
    Assert.assertEquals("partition count not changed", 8, sliceReader.getPartitionCount());
  }

  @Test
  public void testPartitioning() throws Exception
  {
    TestUtils.MockBatchedOperatorStats readerStats = new TestUtils.MockBatchedOperatorStats(2);
    readerStats.operatorStats = Lists.newArrayList();
    readerStats.operatorStats.add(new ReaderStats(10, 1, 100, 1));

    TestReader sliceReader = new TestReader();

    StatsListener.Response response = sliceReader.processStats(readerStats);

    Assert.assertTrue("partition needed", response.repartitionRequired);
    Assert.assertEquals("partition count changed", 8, sliceReader.getPartitionCount());

    List<Partitioner.Partition<AbstractBlockReader<Slice, BlockMetadata.FileBlockMetadata, FSDataInputStream>>>
        partitions = Lists
        .newArrayList();

    DefaultPartition<AbstractBlockReader<Slice, BlockMetadata.FileBlockMetadata, FSDataInputStream>> apartition = new
        DefaultPartition<AbstractBlockReader<Slice, BlockMetadata.FileBlockMetadata, FSDataInputStream>>(
        sliceReader);

    TestUtils.MockPartition<AbstractBlockReader<Slice, BlockMetadata.FileBlockMetadata, FSDataInputStream>>
        pseudoParttion = new TestUtils.MockPartition<>(
        apartition, readerStats);

    partitions.add(pseudoParttion);

    Collection<Partitioner.Partition<AbstractBlockReader<Slice, BlockMetadata.FileBlockMetadata, FSDataInputStream>>>
        newPartitions = sliceReader
        .definePartitions(partitions, null);
    Assert.assertEquals(8, newPartitions.size());
  }

  @Test
  public void testCountersTransfer() throws Exception
  {
    TestUtils.MockBatchedOperatorStats readerStats = new TestUtils.MockBatchedOperatorStats(2);
    readerStats.operatorStats = Lists.newArrayList();
    readerStats.operatorStats.add(new ReaderStats(10, 1, 100, 1));

    TestReader sliceReader = new TestReader();
    sliceReader.processStats(readerStats);

    List<Partitioner.Partition<AbstractBlockReader<Slice, BlockMetadata.FileBlockMetadata, FSDataInputStream>>>
        partitions = Lists
        .newArrayList();

    DefaultPartition<AbstractBlockReader<Slice, BlockMetadata.FileBlockMetadata, FSDataInputStream>> apartition = new
        DefaultPartition<AbstractBlockReader<Slice, BlockMetadata.FileBlockMetadata, FSDataInputStream>>(
        sliceReader);

    TestUtils.MockPartition<AbstractBlockReader<Slice, BlockMetadata.FileBlockMetadata, FSDataInputStream>>
        pseudoParttion = new TestUtils.MockPartition<>(apartition, readerStats);

    partitions.add(pseudoParttion);

    Collection<Partitioner.Partition<AbstractBlockReader<Slice, BlockMetadata.FileBlockMetadata, FSDataInputStream>>>
        newPartitions = sliceReader.definePartitions(partitions, null);

    List<Partitioner.Partition<AbstractBlockReader<Slice, BlockMetadata.FileBlockMetadata, FSDataInputStream>>>
        newMocks = Lists.newArrayList();

    for (Partitioner.Partition<AbstractBlockReader<Slice, BlockMetadata.FileBlockMetadata, FSDataInputStream>> partition
        : newPartitions) {

      partition.getPartitionedInstance().counters
          .setCounter(AbstractBlockReader.ReaderCounterKeys.BLOCKS, new MutableLong(1));

      newMocks.add(new TestUtils.MockPartition<>(
          (DefaultPartition<AbstractBlockReader<Slice, BlockMetadata.FileBlockMetadata, FSDataInputStream>>)partition,
          readerStats));
    }
    sliceReader.partitionCount = 1;
    newPartitions = sliceReader.definePartitions(newMocks, null);
    Assert.assertEquals(1, newPartitions.size());

    AbstractBlockReader<Slice, BlockMetadata.FileBlockMetadata, FSDataInputStream> last = newPartitions.iterator()
        .next().getPartitionedInstance();
    Assert.assertEquals("num blocks", 8,
        last.counters.getCounter(AbstractBlockReader.ReaderCounterKeys.BLOCKS).longValue());
  }

  static class ReaderStats extends Stats.OperatorStats
  {

    ReaderStats(int backlog, long readBlocks, long bytes, long time)
    {
      BasicCounters<MutableLong> bc = new BasicCounters<>(MutableLong.class);
      bc.setCounter(AbstractBlockReader.ReaderCounterKeys.BLOCKS, new MutableLong(readBlocks));
      bc.setCounter(AbstractBlockReader.ReaderCounterKeys.BYTES, new MutableLong(bytes));
      bc.setCounter(AbstractBlockReader.ReaderCounterKeys.TIME, new MutableLong(time));

      counters = bc;

      PortStats portStats = new PortStats("blocks");
      portStats.queueSize = backlog;
      inputPorts = Lists.newArrayList(portStats);
    }
  }

  static class TestReader extends FSSliceReader
  {
    int getPartitionCount()
    {
      return partitionCount;
    }
  }
}
