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
package com.datatorrent.lib.io.fs;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.*;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang.mutable.MutableLong;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.datatorrent.lib.counters.BasicCounters;

import com.datatorrent.api.*;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;

/**
 * AbstractBlockReader processes a block of data from a bigger file.<br/>
 * It works on {@link FileSplitter.BlockMetadata} which provides the block details and can be used to parallelize the processing of data within a file.<br/>
 *
 * <p/>
 * If a record is split across blocks then the reader continues reading across the block boundary until the record is completely read.
 * In that scenario when the next block is parsed, the first record would be partial and so is ignored.
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
 * @param <R> type of records.
 */
public abstract class AbstractBlockReader<R> extends BaseOperator implements
        Partitioner<AbstractBlockReader<R>>, StatsListener, Operator.IdleTimeHandler
{
  protected int operatorId;
  protected transient long windowId;
  protected transient FileSystem fs;
  protected transient Configuration configuration;
  protected transient FSDataInputStream inputStream;

  /**
   * Limit on the no. of blocks to be processed in a window. By default {@link Integer#MAX_VALUE}
   */
  private int threshold;
  private transient int blocksPerWindow;

  protected final BasicCounters<MutableLong> counters;

  private transient Context.OperatorContext context;

  private Queue<FileSplitter.BlockMetadata> blockQueue;

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

  private final Response response;
  private int partitionCount;
  private final Map<Integer, Long> backlogPerOperator;
  private transient long nextMillis;

  public final transient DefaultOutputPort<FileSplitter.BlockMetadata> blocksMetadataOutput = new DefaultOutputPort<FileSplitter.BlockMetadata>();
  public final transient DefaultOutputPort<ReaderRecord<R>> messages = new DefaultOutputPort<ReaderRecord<R>>();

  public final transient DefaultInputPort<FileSplitter.BlockMetadata> blocksMetadataInput = new DefaultInputPort<FileSplitter.BlockMetadata>()
  {
    @Override
    public void process(FileSplitter.BlockMetadata blockMetadata)
    {
      blockQueue.add(blockMetadata);
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
    response = new Response();
    backlogPerOperator = Maps.newHashMap();
    partitionCount = 1;
    threshold = Integer.MAX_VALUE;
    counters = new BasicCounters<MutableLong>(MutableLong.class);
    blockQueue = new LinkedList<FileSplitter.BlockMetadata>();
    collectStats = true;
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
    configuration = new Configuration();
    try {
      fs = getFSInstance();
    }
    catch (IOException e) {
      throw new RuntimeException("creating fs", e);
    }
  }

  /**
   * Override this method to change the FileSystem instance that is used by the operator.
   *
   * @return A FileSystem object.
   * @throws IOException
   */
  protected FileSystem getFSInstance() throws IOException
  {
    return FileSystem.newInstance(configuration);
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
    if (blockQueue.isEmpty() || blocksPerWindow >= threshold) {
      /* nothing to do here, so sleep for a while to avoid busy loop */
      try {
        Thread.sleep(sleepTimeMillis);
      }
      catch (InterruptedException ie) {
        throw new RuntimeException(ie);
      }
    }
    else {
      do {
        processHeadBlock();
      }
      while (blocksPerWindow < threshold && !blockQueue.isEmpty());
    }
  }

  private void processHeadBlock()
  {
    FileSplitter.BlockMetadata top = blockQueue.poll();
    try {
      if (blocksMetadataOutput.isConnected()) {
        blocksMetadataOutput.emit(top);
      }
      processBlockMetadata(top);
      blocksPerWindow++;
    }
    catch (IOException e) {
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

  protected void processBlockMetadata(FileSplitter.BlockMetadata blockMetadata) throws IOException
  {
    long blockStartTime = System.currentTimeMillis();

    initReaderFor(blockMetadata);
    try {
      readBlock(blockMetadata);
    }
    finally {
      closeCurrentReader();
    }
    counters.getCounter(ReaderCounterKeys.TIME).add(System.currentTimeMillis() - blockStartTime);
  }

  /**
   * Override this if you want to change how much of the block is read.
   *
   * @param blockMetadata
   * @throws IOException
   */
  protected void readBlock(FileSplitter.BlockMetadata blockMetadata) throws IOException
  {
    final long blockLength = blockMetadata.getLength();

    long blockOffset = blockMetadata.getOffset();
    while (blockOffset < blockLength) {

      Entity entity = readEntity(blockMetadata, blockOffset);

      //The construction of entity was not complete as record end was never found.
      if (entity == null) {
        break;
      }
      counters.getCounter(ReaderCounterKeys.BYTES).add(entity.usedBytes);
      blockOffset += entity.usedBytes;

      R record = convertToRecord(entity.record);

      //If the record is partial then ignore the record.
      if (isRecordValid(record)) {
        counters.getCounter(ReaderCounterKeys.RECORDS).increment();
        messages.emit(new ReaderRecord<R>(blockMetadata.getBlockId(), record));
      }
    }
  }

  /**
   * Initializes the reading of a block-metadata.
   *
   * @param blockMetadata
   * @throws IOException
   */
  protected void initReaderFor(FileSplitter.BlockMetadata blockMetadata) throws IOException
  {
    LOG.debug("open {}", blockMetadata.getFilePath());
    inputStream = fs.open(new Path(blockMetadata.getFilePath()));
  }

  /**
   * Close the reading of a block-metadata.
   *
   * @throws IOException
   */
  protected void closeCurrentReader() throws IOException
  {
    if (inputStream != null) {
      LOG.debug("close reader");
      inputStream.close();
      inputStream = null;
    }
  }

  /**
   * <b>Note:</b> This partitioner does not support parallel partitioning.<br/><br/>
   * {@inheritDoc}
   */
  @SuppressWarnings("unchecked")
  @Override
  public Collection<Partition<AbstractBlockReader<R>>> definePartitions(Collection<Partition<AbstractBlockReader<R>>> partitions, int incrementalCapacity)
  {
    if (partitions.iterator().next().getStats() == null) {
      //First time when define partitions is called
      return partitions;
    }
    //Collect state here
    List<FileSplitter.BlockMetadata> pendingBlocks = Lists.newArrayList();
    for (Partition<AbstractBlockReader<R>> partition : partitions) {
      pendingBlocks.addAll(partition.getPartitionedInstance().blockQueue);
    }

    int morePartitionsToCreate = partitionCount - partitions.size();

    if (morePartitionsToCreate < 0) {
      //Delete partitions
      Iterator<Partition<AbstractBlockReader<R>>> partitionIterator = partitions.iterator();
      while (morePartitionsToCreate++ < 0) {
        Partition<AbstractBlockReader<R>> toRemove = partitionIterator.next();
        LOG.debug("partition removed {}", toRemove.getPartitionedInstance().operatorId);
        partitionIterator.remove();
      }
    }
    else {
      //Add more partitions
      while (morePartitionsToCreate-- > 0) {
        AbstractBlockReader<R> blockReader;
        try {
          blockReader = this.getClass().newInstance();
        }
        catch (InstantiationException e) {
          throw new RuntimeException(e);
        }
        catch (IllegalAccessException e) {
          throw new RuntimeException(e);
        }
        DefaultPartition<AbstractBlockReader<R>> partition = new DefaultPartition<AbstractBlockReader<R>>(blockReader);
        partitions.add(partition);
      }
    }

    DefaultPartition.assignPartitionKeys(Collections.unmodifiableCollection(partitions), blocksMetadataInput);
    int lPartitionMask = partitions.iterator().next().getPartitionKeys().get(blocksMetadataInput).mask;

    //transfer the state here
    for (Partition<AbstractBlockReader<R>> newPartition : partitions) {
      AbstractBlockReader<R> reader = newPartition.getPartitionedInstance();

      reader.partitionKeys = newPartition.getPartitionKeys().get(blocksMetadataInput).partitions;
      reader.partitionMask = lPartitionMask;
      LOG.debug("partitions {},{}", reader.partitionKeys, reader.partitionMask);
      reader.blockQueue.clear();

      //distribute block-metadatas
      Iterator<FileSplitter.BlockMetadata> pendingBlocksIterator = pendingBlocks.iterator();
      while (pendingBlocksIterator.hasNext()) {
        FileSplitter.BlockMetadata pending = pendingBlocksIterator.next();
        if (reader.partitionKeys.contains(pending.hashCode() & lPartitionMask)) {
          reader.blockQueue.add(pending);
          pendingBlocksIterator.remove();
        }
      }
    }
    return partitions;
  }

  @Override
  public void partitioned(Map<Integer, Partition<AbstractBlockReader<R>>> integerPartitionMap)
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
      long operatorBacklog = 0;
      int queueSize = lastWindowedStats.get(lastWindowedStats.size() - 1).inputPorts.get(0).queueSize;
      if (queueSize > 1) {
        operatorBacklog += queueSize;
      }

      for (int i = lastWindowedStats.size() - 1; i >= 0; i--) {
        if (lastWindowedStats.get(i).counters != null) {
          @SuppressWarnings("unchecked")
          BasicCounters<MutableLong> basicCounters = (BasicCounters<MutableLong>)lastWindowedStats.get(i).counters;
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
    for (Map.Entry<Integer, Long> backlog: backlogPerOperator.entrySet()) {
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
   * Reads an entity. When a record is broken across data block then it is possible
   * that the message is incomplete so the message can be corrupted.
   *
   * @return {@link Entity}. null indicates that there is nothing more to be read.
   * @throws IOException
   */
  protected abstract Entity readEntity(FileSplitter.BlockMetadata blockMetadata, long blockOffset) throws IOException;

  /**
   * Converts the bytes to record.
   *
   * @param bytes
   * @return record
   */
  protected abstract R convertToRecord(byte[] bytes);

  /**
   * When a record is split across blocks then a reader would end up reading a partial record. A partial record is ignored.<br/>
   * Any concrete subclass needs to provide an implementation for validating whether a record is partial or intact.<br/>
   *
   * @param record
   * @return true for a valid record; false otherwise;
   */
  protected abstract boolean isRecordValid(R record);

  /**
   * Sets the maximum number of block readers.
   *
   * @param maxReaders
   */
  public void setMaxReaders(int maxReaders)
  {
    this.maxReaders = maxReaders;
  }

  /**
   * Sets the minimum number of block readers.
   *
   * @param minReaders
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
   *
   * @param threshold
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
   *
   * @param collectStats
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
   * @param intervalMillis
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

  @Override
  public String toString()
  {
    return "Reader{" + "nextMillis=" + nextMillis + ", intervalMillis=" + intervalMillis + '}';
  }

  /**
   * Represents the record and the total bytes used by an {@link AbstractBlockReader} to construct the record.<br/>
   * used bytes can be different from the bytes in the record.
   */
  public static class Entity
  {
    public byte[] record;
    public long usedBytes;

    public void clear()
    {
      record = null;
      usedBytes = -1;
    }

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

  /**
   * An implementation of {@link AbstractBlockReader} that splits the block into records on '\n' or '\r'.<br/>
   * This implementation is based on the assumption that there is a way to validate a record by checking the start of
   * each record.
   *
   * @param <R> type of record.
   */
  public static abstract class AbstractLineReader<R> extends AbstractBlockReader<R>
  {
    protected int bufferSize;

    private final transient ByteArrayOutputStream lineBuilder;
    private final transient ByteArrayOutputStream emptyBuilder;
    private final transient ByteArrayOutputStream tmpBuilder;

    private transient byte[] buffer;
    private transient String strBuffer;
    private transient int posInStr;
    private final transient Entity entity;

    public AbstractLineReader()
    {
      super();
      bufferSize = 8192;
      lineBuilder = new ByteArrayOutputStream();
      emptyBuilder = new ByteArrayOutputStream();
      tmpBuilder = new ByteArrayOutputStream();
      entity = new Entity();
    }

    @Override
    public void setup(Context.OperatorContext context)
    {
      super.setup(context);
      buffer = new byte[bufferSize];
    }

    @Override
    public void partitioned(Map<Integer, Partition<AbstractBlockReader<R>>> integerPartitionMap)
    {
      super.partitioned(integerPartitionMap);
      for (Partition<AbstractBlockReader<R>> partition : integerPartitionMap.values()) {
        ((AbstractLineReader<R>)partition.getPartitionedInstance()).bufferSize = bufferSize;
      }
    }

    @Override
    protected void initReaderFor(FileSplitter.BlockMetadata blockMetadata) throws IOException
    {
      super.initReaderFor(blockMetadata);
      posInStr = 0;
    }

    @Override
    protected Entity readEntity(FileSplitter.BlockMetadata blockMetadata, long blockOffset) throws IOException
    {
      //Implemented a buffered reader instead of using java's BufferedReader because it was reading much ahead of block boundary
      //and faced issues with duplicate records. Controlling the buffer size didn't help either.

      boolean foundEOL = false;
      int bytesRead = 0;
      long usedBytes = 0;

      while (!foundEOL) {
        tmpBuilder.reset();
        if (posInStr == 0) {
          bytesRead = inputStream.read(blockOffset + usedBytes, buffer, 0, bufferSize);
          if (bytesRead == -1) {
            break;
          }
          strBuffer = new String(buffer);
        }

        while (posInStr < strBuffer.length()) {
          char c = strBuffer.charAt(posInStr);
          if (c != '\r' && c != '\n') {
            tmpBuilder.write(c);
            posInStr++;
          }
          else {
            foundEOL = true;
            break;
          }
        }
        byte[] subLine = tmpBuilder.toByteArray();
        usedBytes += subLine.length;
        lineBuilder.write(subLine);

        if (foundEOL) {
          while (posInStr < strBuffer.length()) {
            char c = strBuffer.charAt(posInStr);
            if (c == '\r' || c == '\n') {
              emptyBuilder.write(c);
              posInStr++;
            }
            else {
              break;
            }
          }
          usedBytes += emptyBuilder.toByteArray().length;
        }
        else {
          //read more bytes from the input stream
          posInStr = 0;
        }
      }

      //when end of stream is reached then bytesRead is -1
      if (bytesRead == -1) {
        return null;
      }
      entity.clear();
      entity.record = lineBuilder.toByteArray();
      entity.usedBytes = usedBytes;

      lineBuilder.reset();
      emptyBuilder.reset();

      return entity;
    }

    /**
     * Sets the buffer size of read.
     *
     * @param bufferSize size of the buffer
     */
    public void setBufferSize(int bufferSize)
    {
      this.bufferSize = bufferSize;
    }

    /**
     * @return the buffer size of read.
     */
    public int getBufferSize()
    {
      return this.bufferSize;
    }

    private static final long serialVersionUID = 201410081030L;

    @SuppressWarnings("UnusedDeclaration")
    private static final Logger LOG = LoggerFactory.getLogger(AbstractLineReader.class);
  }

  private static final long serialVersionUID = 201408261653L;

  private static final Logger LOG = LoggerFactory.getLogger(AbstractBlockReader.class);

}
