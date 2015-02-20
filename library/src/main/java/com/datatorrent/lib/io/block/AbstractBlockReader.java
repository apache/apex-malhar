/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.io.block;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

import javax.validation.constraints.NotNull;

import org.apache.commons.lang.mutable.MutableLong;
import org.apache.hadoop.fs.PositionedReadable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import com.datatorrent.api.*;

import com.datatorrent.lib.counters.BasicCounters;

/**
 * AbstractBlockReader processes a block of data from a stream.<br/>
 * It works with {@link BlockMetadata} which provides the block details and can be used to parallelize the processing of
 * data from a source.
 *
 * <p/>
 * The {@link ReaderContext} provides the way to read from the stream. It can control either to always read ahead or
 * stop at the block boundary.
 *
 * <p/>
 * Properties that can be set on AbstractBlockReader:<br/>
 * {@link #threshold}: max number of blocks to be processed in a window.<br/>
 * {@link #collectStats}: the operator is dynamically partition-able which is influenced by the backlog and the port queue size. This property disables
 * collecting stats and thus partitioning.<br/>
 * {@link #maxReaders}: Maximum number of readers when dynamic partitioning is on.<br/>
 * {@link #minReaders}: Minimum number of readers when dynamic partitioning is on.<br/>
 * {@link #intervalMillis}: interval at which stats are processed by the block reader.<br/>
 *
 * @param <R>      type of records.
 * @param <B>      type of blocks.
 * @param <STREAM> type of stream.
 */

public abstract class AbstractBlockReader<R, B extends BlockMetadata, STREAM extends InputStream & PositionedReadable> extends BaseOperator implements
  Partitioner<AbstractBlockReader<R, B, STREAM>>, StatsListener, Operator.IdleTimeHandler
{
  protected int operatorId;
  protected transient long windowId;

  @NotNull
  protected ReaderContext<STREAM> readerContext;
  protected transient STREAM stream;

  /**
   * Limit on the no. of blocks to be processed in a window. By default {@link Integer#MAX_VALUE}
   */
  private int threshold;
  private transient int blocksPerWindow;

  protected final BasicCounters<MutableLong> counters;

  private transient Context.OperatorContext context;

  private final Queue<B> blockQueue;

  private transient long sleepTimeMillis;

  protected Set<Integer> partitionKeys;
  protected int partitionMask;

  //Stats-listener and partition-er properties
  /**
   * Controls stats collections. Default : true
   */
  private boolean collectStats;
  /**
   * Max number of readers. Default : 16
   */
  protected int maxReaders;
  /**
   * Minimum number of readers. Default : 1
   */
  protected int minReaders;
  /**
   * Interval at which stats are processed. Default : 1 minute
   */
  private long intervalMillis;

  private final StatsListener.Response response;
  private int partitionCount;
  private final Map<Integer, Long> backlogPerOperator;
  private transient long nextMillis;

  private transient B lastProcessedBlock;
  private transient long lastBlockOpenTime;
  protected transient boolean consecutiveBlock;

  public final transient DefaultOutputPort<B> blocksMetadataOutput = new DefaultOutputPort<B>();
  public final transient DefaultOutputPort<ReaderRecord<R>> messages = new DefaultOutputPort<ReaderRecord<R>>();

  public final transient DefaultInputPort<B> blocksMetadataInput = new DefaultInputPort<B>()
  {
    @Override
    public void process(B block)
    {
      blockQueue.add(block);
      if (blocksPerWindow < threshold) {
        processHeadBlock();
      }
    }
  };

  public AbstractBlockReader()
  {
    maxReaders = 16;
    minReaders = 1;
    intervalMillis = 60 * 1000L;
    response = new StatsListener.Response();
    backlogPerOperator = Maps.newHashMap();
    partitionCount = 1;
    threshold = Integer.MAX_VALUE;
    counters = new BasicCounters<MutableLong>(MutableLong.class);
    blockQueue = new LinkedList<B>();
    collectStats = true;
    lastBlockOpenTime = -1;
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    operatorId = context.getId();
    LOG.debug("{}: partition keys {} mask {}", operatorId, partitionKeys, partitionMask);

    this.context = context;
    counters.setCounter(ReaderCounterKeys.BLOCKS, new MutableLong());
    counters.setCounter(ReaderCounterKeys.RECORDS, new MutableLong());
    counters.setCounter(ReaderCounterKeys.BYTES, new MutableLong());
    counters.setCounter(ReaderCounterKeys.TIME, new MutableLong());
    counters.setCounter(ReaderCounterKeys.BACKLOG, new MutableLong());
    sleepTimeMillis = context.getValue(Context.OperatorContext.SPIN_MILLIS);
  }

  @Override
  public void beginWindow(long windowId)
  {
    this.windowId = windowId;
    blocksPerWindow = 0;
  }

  @Override
  public void handleIdleTime()
  {
    if (!blockQueue.isEmpty() && blocksPerWindow < threshold) {
      do {
        processHeadBlock();
      }
      while (blocksPerWindow < threshold && !blockQueue.isEmpty());
    }
    else if (lastProcessedBlock != null && System.currentTimeMillis() - lastBlockOpenTime > intervalMillis) {
      try {
        teardownStream(lastProcessedBlock);
        lastProcessedBlock = null;
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    else {
      /* nothing to do here, so sleep for a while to avoid busy loop */
      try {
        Thread.sleep(sleepTimeMillis);
      }
      catch (InterruptedException ie) {
        throw new RuntimeException(ie);
      }
    }
  }

  private void processHeadBlock()
  {
    B top = blockQueue.poll();
    try {
      if (blocksMetadataOutput.isConnected()) {
        blocksMetadataOutput.emit(top);
      }
      processBlockMetadata(top);
      blocksPerWindow++;
    }
    catch (IOException e) {
      try {
        if (lastProcessedBlock != null) {
          teardownStream(lastProcessedBlock);
          lastProcessedBlock = null;
        }
      }
      catch (IOException ie) {
        throw new RuntimeException("closing", ie);
      }
      throw new RuntimeException(e);
    }
  }

  @Override
  public void endWindow()
  {
    counters.getCounter(ReaderCounterKeys.BLOCKS).add(blocksPerWindow);
    counters.getCounter(ReaderCounterKeys.BACKLOG).setValue(blockQueue.size());
    context.setCounters(counters);
  }

  protected void processBlockMetadata(B block) throws IOException
  {
    long blockStartTime = System.currentTimeMillis();
    if (block.getPreviousBlockId() == -1 || lastProcessedBlock == null || block.getPreviousBlockId() != lastProcessedBlock.getBlockId()) {
      teardownStream(lastProcessedBlock);
      consecutiveBlock = false;
      lastBlockOpenTime = System.currentTimeMillis();
      stream = setupStream(block);
    }
    else {
      consecutiveBlock = true;
    }
    readBlock(block);
    lastProcessedBlock = block;
    counters.getCounter(ReaderCounterKeys.TIME).add(System.currentTimeMillis() - blockStartTime);
  }

  /**
   * Override this if you want to change how much of the block is read.
   *
   * @param blockMetadata block
   * @throws IOException
   */
  protected void readBlock(BlockMetadata blockMetadata) throws IOException
  {
    readerContext.initialize(stream, blockMetadata, consecutiveBlock);
    ReaderContext.Entity entity;
    while ((entity = readerContext.next()) != null) {

      counters.getCounter(ReaderCounterKeys.BYTES).add(entity.getUsedBytes());

      R record = convertToRecord(entity.getRecord());

      //If the record is partial then ignore the record.
      if (record != null) {
        counters.getCounter(ReaderCounterKeys.RECORDS).increment();
        messages.emit(new ReaderRecord<R>(blockMetadata.getBlockId(), record));
      }
    }
  }

  /**
   * <b>Note:</b> This partitioner does not support parallel partitioning.<br/><br/>
   * {@inheritDoc}
   */
  @SuppressWarnings("unchecked")
  @Override
  public Collection<Partition<AbstractBlockReader<R, B, STREAM>>> definePartitions(Collection<Partition<AbstractBlockReader<R, B, STREAM>>> partitions, PartitioningContext context)
  {
    if (partitions.iterator().next().getStats() == null) {
      //First time when define partitions is called
      return partitions;
    }
    //Collect state here
    List<B> pendingBlocks = Lists.newArrayList();
    for (Partition<AbstractBlockReader<R, B, STREAM>> partition : partitions) {
      pendingBlocks.addAll(partition.getPartitionedInstance().blockQueue);
    }

    int morePartitionsToCreate = partitionCount - partitions.size();

    if (morePartitionsToCreate < 0) {
      //Delete partitions
      Iterator<Partition<AbstractBlockReader<R, B, STREAM>>> partitionIterator = partitions.iterator();
      while (morePartitionsToCreate++ < 0) {
        Partition<AbstractBlockReader<R, B, STREAM>> toRemove = partitionIterator.next();
        LOG.debug("partition removed {}", toRemove.getPartitionedInstance().operatorId);
        partitionIterator.remove();
      }
    }
    else {
      //Add more partitions
      while (morePartitionsToCreate-- > 0) {
        AbstractBlockReader<R, B, STREAM> blockReader;
        try {
          blockReader = this.getClass().newInstance();
        }
        catch (InstantiationException e) {
          throw new RuntimeException(e);
        }
        catch (IllegalAccessException e) {
          throw new RuntimeException(e);
        }
        DefaultPartition<AbstractBlockReader<R, B, STREAM>> partition = new DefaultPartition<AbstractBlockReader<R, B, STREAM>>(blockReader);
        partitions.add(partition);
      }
    }

    DefaultPartition.assignPartitionKeys(Collections.unmodifiableCollection(partitions), blocksMetadataInput);
    int lPartitionMask = partitions.iterator().next().getPartitionKeys().get(blocksMetadataInput).mask;

    //transfer the state here
    for (Partition<AbstractBlockReader<R, B, STREAM>> newPartition : partitions) {
      AbstractBlockReader<R, B, STREAM> reader = newPartition.getPartitionedInstance();

      reader.partitionKeys = newPartition.getPartitionKeys().get(blocksMetadataInput).partitions;
      reader.partitionMask = lPartitionMask;
      LOG.debug("partitions {},{}", reader.partitionKeys, reader.partitionMask);
      reader.blockQueue.clear();

      //distribute block-metadatas
      Iterator<B> pendingBlocksIterator = pendingBlocks.iterator();
      while (pendingBlocksIterator.hasNext()) {
        B pending = pendingBlocksIterator.next();
        if (reader.partitionKeys.contains(pending.hashCode() & lPartitionMask)) {
          reader.blockQueue.add(pending);
          pendingBlocksIterator.remove();
        }
      }
    }
    return partitions;
  }

  @Override
  public void partitioned(Map<Integer, Partition<AbstractBlockReader<R, B, STREAM>>> integerPartitionMap)
  {
    for (Partition<AbstractBlockReader<R, B, STREAM>> partition : integerPartitionMap.values()) {
      partition.getPartitionedInstance().readerContext = this.readerContext;
    }
  }

  @Override
  public Response processStats(BatchedOperatorStats stats)
  {
    response.repartitionRequired = false;
    if (!collectStats) {
      return response;
    }

    List<Stats.OperatorStats> lastWindowedStats = stats.getLastWindowedStats();
    if (lastWindowedStats != null && lastWindowedStats.size() > 0) {
      long operatorBacklog = 0;
      int queueSize = lastWindowedStats.get(lastWindowedStats.size() - 1).inputPorts.get(0).queueSize;
      if (queueSize > 1) {
        operatorBacklog += queueSize;
      }

      for (int i = lastWindowedStats.size() - 1; i >= 0; i--) {
        if (lastWindowedStats.get(i).counters != null) {
          @SuppressWarnings("unchecked")
          BasicCounters<MutableLong> basicCounters = (BasicCounters<MutableLong>) lastWindowedStats.get(i).counters;
          operatorBacklog += basicCounters.getCounter(ReaderCounterKeys.BACKLOG).longValue();
          break;
        }
      }
      backlogPerOperator.put(stats.getOperatorId(), operatorBacklog);
    }

    if (System.currentTimeMillis() < nextMillis) {
      return response;
    }

    LOG.debug("nextMillis = {}", nextMillis);
    long totalBacklog = 0;
    for (Map.Entry<Integer, Long> backlog : backlogPerOperator.entrySet()) {
      totalBacklog += backlog.getValue();
    }
    LOG.debug("backlog {} partitionCount {}", totalBacklog, partitionCount);
    backlogPerOperator.clear();

    if (totalBacklog == partitionCount) {
      return response; //do not repartition
    }

    int newPartitionCount = 1;
    if (totalBacklog > maxReaders) {
      LOG.debug("large backlog {}", totalBacklog);
      newPartitionCount = maxReaders;
    }
    else if (totalBacklog < minReaders) {
      LOG.debug("small backlog {}", totalBacklog);
      newPartitionCount = minReaders;
    }
    else {
      while (newPartitionCount < totalBacklog) {
        newPartitionCount <<= 1;
      }
      if (newPartitionCount > maxReaders) {
        newPartitionCount = maxReaders;
      }
      else if (newPartitionCount < minReaders) {
        newPartitionCount = minReaders;
      }
      LOG.debug("moderate backlog {}", totalBacklog);
    }

    LOG.debug("backlog {} newPartitionCount {} partitionCount {}", totalBacklog, newPartitionCount, partitionCount);
    if (newPartitionCount == partitionCount) {
      return response; //do not repartition
    }

    partitionCount = newPartitionCount;
    response.repartitionRequired = true;
    LOG.debug("partition required", totalBacklog, partitionCount);

    nextMillis = System.currentTimeMillis() + intervalMillis;
    LOG.debug("Proposed NextMillis = {}", nextMillis);

    return response;
  }

  /**
   * Initializes the reading of a block and seek to an offset.
   *
   * @throws IOException
   */
  protected abstract STREAM setupStream(B block) throws IOException;

  /**
   * Close the reading of a block-metadata.
   *
   * @throws IOException
   */
  protected void teardownStream(@SuppressWarnings("unused") B block) throws IOException
  {
    if (stream != null) {
      stream.close();
      stream = null;
    }
  }

  /**
   * Converts the bytes to record. This return null when either the bytes were insufficient to convert the record,
   * a record was invalid after the conversion or it failed a business logic validation. <br/>
   *
   * @param bytes bytes
   * @return record
   */
  protected abstract R convertToRecord(byte[] bytes);

  /**
   * Sets the maximum number of block readers.
   *
   * @param maxReaders max number of readers.
   */
  public void setMaxReaders(int maxReaders)
  {
    this.maxReaders = maxReaders;
  }

  /**
   * Sets the minimum number of block readers.
   *
   * @param minReaders min number of readers.
   */
  public void setMinReaders(int minReaders)
  {
    this.minReaders = minReaders;
  }

  /**
   * @return maximum instances of block reader.
   */
  public int getMaxReaders()
  {
    return maxReaders;
  }

  /**
   * @return minimum instances of block reader.
   */
  public int getMinReaders()
  {
    return minReaders;
  }

  /**
   * Sets the threshold on the number of blocks that can be processed in a window.
   */
  public void setThreshold(Integer threshold)
  {
    this.threshold = threshold;
  }

  /**
   * @return threshold on the number of blocks that can be processed in a window.
   */
  public Integer getThreshold()
  {
    return threshold;
  }

  /**
   * Enables/disables the block reader to collect stats and partition itself.
   */
  public void setCollectStats(boolean collectStats)
  {
    this.collectStats = collectStats;
  }

  /**
   * @return if collection of stat is enabled or disabled.
   */
  public boolean isCollectStats()
  {
    return collectStats;
  }

  /**
   * Sets the interval in millis at which the stats are processed by the reader.
   *
   * @param intervalMillis interval in milliseconds.
   */
  public void setIntervalMillis(long intervalMillis)
  {
    this.intervalMillis = intervalMillis;
  }

  /**
   * @return the interval in millis at the which stats are processed by the reader.
   */
  public long getIntervalMillis()
  {
    return intervalMillis;
  }

  public void setReaderContext(ReaderContext<STREAM> readerContext)
  {
    this.readerContext = readerContext;
  }

  public ReaderContext<STREAM> getReaderContext()
  {
    return readerContext;
  }

  @Override
  public String toString()
  {
    return "Reader{" + "nextMillis=" + nextMillis + ", intervalMillis=" + intervalMillis + '}';
  }

  /**
   * ReaderRecord wraps the record with the blockId and the reader emits object of this type.
   *
   * @param <R>
   */
  public static class ReaderRecord<R>
  {
    private final long blockId;
    private final R record;

    @SuppressWarnings("unused")
    private ReaderRecord()
    {
      this.blockId = -1;
      this.record = null;
    }

    public ReaderRecord(long blockId, R record)
    {
      this.blockId = blockId;
      this.record = record;
    }

    public long getBlockId()
    {
      return blockId;
    }

    public R getRecord()
    {
      return record;
    }

  }

  public static enum ReaderCounterKeys
  {
    RECORDS, BLOCKS, BYTES, TIME, BACKLOG
  }

  private static final Logger LOG = LoggerFactory.getLogger(AbstractBlockReader.class);

}
