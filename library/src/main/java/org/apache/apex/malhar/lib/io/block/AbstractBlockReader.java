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

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.counters.BasicCounters;
import org.apache.apex.malhar.lib.util.KryoCloneUtils;
import org.apache.commons.lang.mutable.MutableLong;
import org.apache.hadoop.fs.PositionedReadable;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import com.datatorrent.api.AutoMetric;
import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.DefaultPartition;
import com.datatorrent.api.Operator;
import com.datatorrent.api.Partitioner;
import com.datatorrent.api.Stats;
import com.datatorrent.api.StatsListener;
import com.datatorrent.common.util.BaseOperator;

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
 * {@link #collectStats}: the operator is dynamically partition-able which is influenced by the backlog and the port
 * queue size. This property disables
 * collecting stats and thus partitioning.<br/>
 * {@link #maxReaders}: Maximum number of readers when dynamic partitioning is on.<br/>
 * {@link #minReaders}: Minimum number of readers when dynamic partitioning is on.<br/>
 * {@link #intervalMillis}: interval at which stats are processed by the block reader.<br/>
 *
 * <p/>
 * It emits a {@link ReaderRecord} which wraps the record and the block id of the record.
 *
 * @param <R>      type of records.
 * @param <B>      type of blocks.
 * @param <STREAM> type of stream.
 *
 * @since 2.1.0
 */
@StatsListener.DataQueueSize
public abstract class AbstractBlockReader<R, B extends BlockMetadata, STREAM extends InputStream & PositionedReadable>
    extends BaseOperator
    implements Partitioner<AbstractBlockReader<R, B, STREAM>>, StatsListener, Operator.IdleTimeHandler
{
  protected int operatorId;
  protected transient long windowId;

  @NotNull
  protected ReaderContext<STREAM> readerContext;
  protected transient STREAM stream;

  protected transient int blocksPerWindow;

  protected final BasicCounters<MutableLong> counters;

  protected transient Context.OperatorContext context;

  protected transient long sleepTimeMillis;

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
   * Interval at which stats are processed. Default : 2 minutes
   */
  protected long intervalMillis;

  protected final transient StatsListener.Response response;
  protected transient int partitionCount;
  protected final transient Map<Integer, Integer> backlogPerOperator;
  private transient long nextMillis;

  protected transient B lastProcessedBlock;
  protected transient long lastBlockOpenTime;
  protected transient boolean consecutiveBlock;

  @AutoMetric
  private long bytesRead;

  public final transient DefaultOutputPort<B> blocksMetadataOutput = new DefaultOutputPort<>();
  public final transient DefaultOutputPort<ReaderRecord<R>> messages = new DefaultOutputPort<>();

  public final transient DefaultInputPort<B> blocksMetadataInput = new DefaultInputPort<B>()
  {
    @Override
    public void process(B block)
    {
      processBlockMetadata(block);
    }
  };

  public AbstractBlockReader()
  {
    maxReaders = 16;
    minReaders = 1;
    intervalMillis = 2 * 60 * 1000L;
    response = new StatsListener.Response();
    backlogPerOperator = Maps.newHashMap();
    partitionCount = 1;
    counters = new BasicCounters<>(MutableLong.class);
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
    sleepTimeMillis = context.getValue(Context.OperatorContext.SPIN_MILLIS);
  }

  @Override
  public void beginWindow(long windowId)
  {
    this.windowId = windowId;
    blocksPerWindow = 0;
    bytesRead = 0;
  }

  @Override
  public void handleIdleTime()
  {
    if (lastProcessedBlock != null && System.currentTimeMillis() - lastBlockOpenTime > intervalMillis) {
      try {
        teardownStream(lastProcessedBlock);
        lastProcessedBlock = null;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    } else {
      /* nothing to do here, so sleep for a while to avoid busy loop */
      try {
        Thread.sleep(sleepTimeMillis);
      } catch (InterruptedException ie) {
        throw new RuntimeException(ie);
      }
    }
  }

  @Override
  public void endWindow()
  {
    counters.getCounter(ReaderCounterKeys.BLOCKS).add(blocksPerWindow);
    context.setCounters(counters);
  }

  protected void processBlockMetadata(B block)
  {
    try {
      long blockStartTime = System.currentTimeMillis();
      if (block.getPreviousBlockId() == -1 || lastProcessedBlock == null
          || block.getPreviousBlockId() != lastProcessedBlock.getBlockId()) {
        teardownStream(lastProcessedBlock);
        consecutiveBlock = false;
        lastBlockOpenTime = System.currentTimeMillis();
        stream = setupStream(block);
      } else {
        consecutiveBlock = true;
      }
      readBlock(block);
      lastProcessedBlock = block;
      counters.getCounter(ReaderCounterKeys.TIME).add(System.currentTimeMillis() - blockStartTime);
      //emit block metadata only when the block finishes
      if (blocksMetadataOutput.isConnected()) {
        blocksMetadataOutput.emit(block);
      }
      blocksPerWindow++;
    } catch (IOException ie) {
      try {
        if (lastProcessedBlock != null) {
          teardownStream(lastProcessedBlock);
          lastProcessedBlock = null;
        }
      } catch (IOException ioe) {
        throw new RuntimeException("closing last", ie);
      }
      throw new RuntimeException(ie);
    }
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
      bytesRead += entity.getUsedBytes();

      R record = convertToRecord(entity.getRecord());

      //If the record is partial then ignore the record.
      if (record != null) {
        counters.getCounter(ReaderCounterKeys.RECORDS).increment();
        messages.emit(new ReaderRecord<>(blockMetadata.getBlockId(), record));
      }
    }
  }

  /**
   * <b>Note:</b> This partitioner does not support parallel partitioning.<br/><br/>
   * {@inheritDoc}
   */
  @SuppressWarnings("unchecked")
  @Override
  public Collection<Partition<AbstractBlockReader<R, B, STREAM>>> definePartitions(
      Collection<Partition<AbstractBlockReader<R, B, STREAM>>> partitions, PartitioningContext context)
  {
    if (partitions.iterator().next().getStats() == null) {
      //First time when define partitions is called
      return partitions;
    }
    List<Partition<AbstractBlockReader<R, B, STREAM>>> newPartitions = Lists.newArrayList();

    //Create new partitions
    for (Partition<AbstractBlockReader<R, B, STREAM>> partition : partitions) {
      newPartitions.add(new DefaultPartition<>(partition.getPartitionedInstance()));
    }
    partitions.clear();
    int morePartitionsToCreate = partitionCount - newPartitions.size();
    List<BasicCounters<MutableLong>> deletedCounters = Lists.newArrayList();

    if (morePartitionsToCreate < 0) {
      //Delete partitions
      Iterator<Partition<AbstractBlockReader<R, B, STREAM>>> partitionIterator = newPartitions.iterator();
      while (morePartitionsToCreate++ < 0) {
        Partition<AbstractBlockReader<R, B, STREAM>> toRemove = partitionIterator.next();
        deletedCounters.add(toRemove.getPartitionedInstance().counters);

        LOG.debug("partition removed {}", toRemove.getPartitionedInstance().operatorId);
        partitionIterator.remove();
      }
    } else {
      KryoCloneUtils<AbstractBlockReader<R, B, STREAM>> cloneUtils = KryoCloneUtils.createCloneUtils(this);
      while (morePartitionsToCreate-- > 0) {
        DefaultPartition<AbstractBlockReader<R, B, STREAM>> partition = new DefaultPartition<>(cloneUtils.getClone());
        newPartitions.add(partition);
      }
    }

    DefaultPartition.assignPartitionKeys(Collections.unmodifiableCollection(newPartitions), blocksMetadataInput);
    int lPartitionMask = newPartitions.iterator().next().getPartitionKeys().get(blocksMetadataInput).mask;

    //transfer the state here
    for (Partition<AbstractBlockReader<R, B, STREAM>> newPartition : newPartitions) {
      AbstractBlockReader<R, B, STREAM> reader = newPartition.getPartitionedInstance();

      reader.partitionKeys = newPartition.getPartitionKeys().get(blocksMetadataInput).partitions;
      reader.partitionMask = lPartitionMask;
      LOG.debug("partitions {},{}", reader.partitionKeys, reader.partitionMask);
    }
    //transfer the counters
    AbstractBlockReader<R, B, STREAM> targetReader = newPartitions.iterator().next().getPartitionedInstance();
    for (BasicCounters<MutableLong> removedCounter : deletedCounters) {
      addCounters(targetReader.counters, removedCounter);
    }

    return newPartitions;
  }

  /**
   * Transfers the counters in partitioning.
   *
   * @param target target counter
   * @param source removed counter
   */
  protected void addCounters(BasicCounters<MutableLong> target, BasicCounters<MutableLong> source)
  {
    for (Enum<ReaderCounterKeys> key : ReaderCounterKeys.values()) {
      MutableLong tcounter = target.getCounter(key);
      if (tcounter == null) {
        tcounter = new MutableLong();
        target.setCounter(key, tcounter);
      }
      MutableLong scounter = source.getCounter(key);
      if (scounter != null) {
        tcounter.add(scounter.longValue());
      }
    }
  }

  @Override
  public void partitioned(Map<Integer, Partition<AbstractBlockReader<R, B, STREAM>>> integerPartitionMap)
  {
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
      Stats.OperatorStats lastStats = lastWindowedStats.get(lastWindowedStats.size() - 1);
      if (lastStats.inputPorts.size() > 0) {
        backlogPerOperator.put(stats.getOperatorId(), lastStats.inputPorts.get(0).queueSize);
      }
    }

    if (System.currentTimeMillis() < nextMillis) {
      return response;
    }
    nextMillis = System.currentTimeMillis() + intervalMillis;
    LOG.debug("Proposed NextMillis = {}", nextMillis);

    long totalBacklog = 0;
    for (Map.Entry<Integer, Integer> backlog : backlogPerOperator.entrySet()) {
      totalBacklog += backlog.getValue();
    }
    LOG.debug("backlog {} partitionCount {}", totalBacklog, partitionCount);
    backlogPerOperator.clear();

    if (totalBacklog == partitionCount) {
      return response; //do not repartition
    }

    int newPartitionCount;
    if (totalBacklog > maxReaders) {
      LOG.debug("large backlog {}", totalBacklog);
      newPartitionCount = maxReaders;
    } else if (totalBacklog < minReaders) {
      LOG.debug("small backlog {}", totalBacklog);
      newPartitionCount = minReaders;
    } else {
      newPartitionCount = getAdjustedCount(totalBacklog);
      LOG.debug("moderate backlog {}", totalBacklog);
    }

    LOG.debug("backlog {} newPartitionCount {} partitionCount {}", totalBacklog, newPartitionCount, partitionCount);
    if (newPartitionCount == partitionCount) {
      return response; //do not repartition
    }

    partitionCount = newPartitionCount;
    response.repartitionRequired = true;
    LOG.debug("partition required", totalBacklog, partitionCount);

    return response;
  }

  protected int getAdjustedCount(long newCount)
  {
    int adjustCount = 1;
    while (adjustCount < newCount) {
      adjustCount <<= 1;
    }
    if (adjustCount > newCount) {
      adjustCount >>>= 1;
    }
    LOG.debug("adjust {} => {}", newCount, adjustCount);
    return adjustCount;
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

  public enum ReaderCounterKeys
  {
    RECORDS, BLOCKS, BYTES, TIME
  }

  private static final Logger LOG = LoggerFactory.getLogger(AbstractBlockReader.class);

}
