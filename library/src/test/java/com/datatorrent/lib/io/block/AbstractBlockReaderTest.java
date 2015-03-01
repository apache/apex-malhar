package com.datatorrent.lib.io.block;

import java.util.Collection;
import java.util.List;

import org.apache.commons.lang.mutable.MutableLong;
import org.apache.hadoop.fs.FSDataInputStream;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Lists;

import com.datatorrent.api.DefaultPartition;
import com.datatorrent.api.Partitioner;
import com.datatorrent.api.Stats;
import com.datatorrent.api.StatsListener;

import com.datatorrent.common.util.Slice;
import com.datatorrent.lib.counters.BasicCounters;
import com.datatorrent.lib.util.TestUtils;

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

    List<Partitioner.Partition<AbstractBlockReader<Slice,
      BlockMetadata.FileBlockMetadata, FSDataInputStream>>> partitions = Lists.newArrayList();

    DefaultPartition<AbstractBlockReader<Slice, BlockMetadata.FileBlockMetadata, FSDataInputStream>> apartition =
      new DefaultPartition<AbstractBlockReader<Slice, BlockMetadata.FileBlockMetadata, FSDataInputStream>>(sliceReader);

    TestUtils.MockPartition<AbstractBlockReader<Slice, BlockMetadata.FileBlockMetadata, FSDataInputStream>> pseudoParttion =
      new TestUtils.MockPartition<AbstractBlockReader<Slice, BlockMetadata.FileBlockMetadata, FSDataInputStream>>(apartition, readerStats);

    partitions.add(pseudoParttion);

    Collection<Partitioner.Partition<AbstractBlockReader<Slice,
      BlockMetadata.FileBlockMetadata, FSDataInputStream>>> newPartitions = sliceReader.definePartitions(partitions, null);
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

    List<Partitioner.Partition<AbstractBlockReader<Slice,
      BlockMetadata.FileBlockMetadata, FSDataInputStream>>> partitions = Lists.newArrayList();

    DefaultPartition<AbstractBlockReader<Slice, BlockMetadata.FileBlockMetadata, FSDataInputStream>> apartition =
      new DefaultPartition<AbstractBlockReader<Slice, BlockMetadata.FileBlockMetadata, FSDataInputStream>>(sliceReader);

    TestUtils.MockPartition<AbstractBlockReader<Slice, BlockMetadata.FileBlockMetadata, FSDataInputStream>> pseudoParttion =
      new TestUtils.MockPartition<AbstractBlockReader<Slice, BlockMetadata.FileBlockMetadata, FSDataInputStream>>(apartition, readerStats);

    partitions.add(pseudoParttion);

    Collection<Partitioner.Partition<AbstractBlockReader<Slice,
      BlockMetadata.FileBlockMetadata, FSDataInputStream>>> newPartitions = sliceReader.definePartitions(partitions, null);

    List<Partitioner.Partition<AbstractBlockReader<Slice,
      BlockMetadata.FileBlockMetadata, FSDataInputStream>>> newMocks = Lists.newArrayList();

    for (Partitioner.Partition<AbstractBlockReader<Slice, BlockMetadata.FileBlockMetadata, FSDataInputStream>> partition :
      newPartitions) {
      partition.getPartitionedInstance().counters.setCounter(AbstractBlockReader.ReaderCounterKeys.BLOCKS, new MutableLong(1));

      newMocks.add(
        new TestUtils.MockPartition<AbstractBlockReader<Slice, BlockMetadata.FileBlockMetadata, FSDataInputStream>>(
          (DefaultPartition<AbstractBlockReader<Slice, BlockMetadata.FileBlockMetadata, FSDataInputStream>>) partition,
          readerStats)
      );
    }
    sliceReader.partitionCount = 1;
    newPartitions = sliceReader.definePartitions(newMocks, null);
    Assert.assertEquals(1, newPartitions.size());

    AbstractBlockReader<Slice, BlockMetadata.FileBlockMetadata, FSDataInputStream> last = newPartitions.iterator().next().getPartitionedInstance();
    Assert.assertEquals("num blocks", 8, last.counters.getCounter(AbstractBlockReader.ReaderCounterKeys.BLOCKS).longValue());
  }

  static class ReaderStats extends Stats.OperatorStats
  {

    ReaderStats(long backlog, long readBlocks, long bytes, long time)
    {
      BasicCounters<MutableLong> bc = new BasicCounters<MutableLong>(MutableLong.class);
      bc.setCounter(AbstractBlockReader.ReaderCounterKeys.BACKLOG, new MutableLong(backlog));
      bc.setCounter(AbstractBlockReader.ReaderCounterKeys.BLOCKS, new MutableLong(readBlocks));
      bc.setCounter(AbstractBlockReader.ReaderCounterKeys.BYTES, new MutableLong(bytes));
      bc.setCounter(AbstractBlockReader.ReaderCounterKeys.TIME, new MutableLong(time));

      counters = bc;

      PortStats portStats = new PortStats("blocks");
      portStats.queueSize = 0;
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