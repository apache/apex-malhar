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

    PseudoBatchedOperatorStats readerStats = new PseudoBatchedOperatorStats(2);
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

    PseudoBatchedOperatorStats readerStats = new PseudoBatchedOperatorStats(2);
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
    PseudoBatchedOperatorStats readerStats = new PseudoBatchedOperatorStats(2);
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

    PseudoParttion pseudoParttion = new PseudoParttion(apartition, readerStats);

    partitions.add(pseudoParttion);

    Collection<Partitioner.Partition<AbstractBlockReader<Slice,
      BlockMetadata.FileBlockMetadata, FSDataInputStream>>> newPartitions = sliceReader.definePartitions(partitions, null);
    Assert.assertEquals(8, newPartitions.size());
  }

  static class PseudoBatchedOperatorStats implements StatsListener.BatchedOperatorStats
  {

    final int operatorId;
    List<Stats.OperatorStats> operatorStats;

    PseudoBatchedOperatorStats(int operatorId)
    {
      this.operatorId = operatorId;
    }

    @Override
    public List<Stats.OperatorStats> getLastWindowedStats()
    {
      return operatorStats;
    }

    @Override
    public int getOperatorId()
    {
      return 0;
    }

    @Override
    public long getCurrentWindowId()
    {
      return 0;
    }

    @Override
    public long getTuplesProcessedPSMA()
    {
      return 0;
    }

    @Override
    public long getTuplesEmittedPSMA()
    {
      return 0;
    }

    @Override
    public double getCpuPercentageMA()
    {
      return 0;
    }

    @Override
    public long getLatencyMA()
    {
      return 0;
    }
  }

  static class PseudoParttion extends DefaultPartition<AbstractBlockReader<Slice,
    BlockMetadata.FileBlockMetadata, FSDataInputStream>>
  {

    PseudoParttion(DefaultPartition<AbstractBlockReader<Slice, BlockMetadata.FileBlockMetadata,
      FSDataInputStream>> defaultPartition, StatsListener.BatchedOperatorStats stats)
    {
      super(defaultPartition.getPartitionedInstance(), defaultPartition.getPartitionKeys(),
        defaultPartition.getLoad(), stats);

    }
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