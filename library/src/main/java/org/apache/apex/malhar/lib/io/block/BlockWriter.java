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
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.counters.BasicCounters;
import org.apache.apex.malhar.lib.io.fs.AbstractFileOutputOperator;
import org.apache.commons.lang.mutable.MutableLong;
import org.apache.hadoop.fs.Path;

import com.google.common.collect.Lists;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.DefaultPartition;
import com.datatorrent.api.Partitioner;
import com.datatorrent.netlet.util.Slice;

/**
 * Writes a block to the appFS (HDFS on which app is running). This is temporary
 * write to HDFS to handle large files.
 *
 * @since 3.4.0
 */
public class BlockWriter extends AbstractFileOutputOperator<AbstractBlockReader.ReaderRecord<Slice>>
    implements Partitioner<BlockWriter>
{
  /**
   * Default value for blocksDirectory
   */
  public static final String DEFAULT_BLOCKS_DIR = "blocks";
  /**
   * Directory under application directory where blocks gets stored
   */
  private String blocksDirectory = DEFAULT_BLOCKS_DIR;

  /**
   * List of FileBlockMetadata received in the current window.
   */
  private transient List<BlockMetadata.FileBlockMetadata> blockMetadatas;

  /**
   * Input port to receive Block meta data
   */
  public final transient DefaultInputPort<BlockMetadata.FileBlockMetadata> blockMetadataInput = new DefaultInputPort<BlockMetadata.FileBlockMetadata>()
  {

    @Override
    public void process(BlockMetadata.FileBlockMetadata blockMetadata)
    {
      blockMetadatas.add(blockMetadata);
      LOG.debug("received blockId {} for file {} ", blockMetadata.getBlockId(), blockMetadata.getFilePath());
    }
  };

  /**
   * Output port to send Block meta data to downstream operator
   */
  public final transient DefaultOutputPort<BlockMetadata.FileBlockMetadata> blockMetadataOutput = new DefaultOutputPort<BlockMetadata.FileBlockMetadata>();

  public BlockWriter()
  {
    super();
    blockMetadatas = Lists.newArrayList();
    //The base class puts a restriction that the file-path cannot be null. With this block writer it is
    //being initialized in setup and not through configuration. So setting it to empty string.
    filePath = "";
  }

  /**
   * Also, initializes the filePath based on Application path
   */
  @Override
  public void setup(Context.OperatorContext context)
  {
    filePath = context.getValue(Context.DAGContext.APPLICATION_PATH) + Path.SEPARATOR + blocksDirectory;
    super.setup(context);
  }

  /**
   * Finalizes files for all the blockMetaDatas received during current window
   */
  @Override
  public void endWindow()
  {
    super.endWindow();

    streamsCache.asMap().clear();
    endOffsets.clear();

    for (BlockMetadata.FileBlockMetadata blockMetadata : blockMetadatas) {
      try {
        finalizeFile(Long.toString(blockMetadata.getBlockId()));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      blockMetadataOutput.emit(blockMetadata);
    }
    blockMetadatas.clear();
  }

  @Override
  protected String getFileName(AbstractBlockReader.ReaderRecord<Slice> tuple)
  {
    return Long.toString(tuple.getBlockId());
  }

  @Override
  protected byte[] getBytesForTuple(AbstractBlockReader.ReaderRecord<Slice> tuple)
  {
    return tuple.getRecord().buffer;
  }


  @Override
  public Collection<Partition<BlockWriter>> definePartitions(Collection<Partition<BlockWriter>> partitions,
      PartitioningContext context)
  {
    if (context.getParallelPartitionCount() == 0) {
      return partitions;
    }

    // if there is no change of count, return the same collection
    if (context.getParallelPartitionCount() == partitions.size()) {
      LOG.debug("no change is partition count: " + partitions.size());
      return partitions;
    }

    List<BasicCounters<MutableLong>> deletedCounters = Lists.newArrayList();

    LOG.debug("block writer parallel partition count {}", context.getParallelPartitionCount());
    int morePartitionsToCreate = context.getParallelPartitionCount() - partitions.size();
    if (morePartitionsToCreate < 0) {
      //Delete partitions
      Iterator<Partition<BlockWriter>> partitionIterator = partitions.iterator();

      while (morePartitionsToCreate++ < 0) {
        Partition<BlockWriter> toRemove = partitionIterator.next();
        deletedCounters.add(toRemove.getPartitionedInstance().fileCounters);
        partitionIterator.remove();
      }
    } else {
      //Add more partitions
      BlockWriter anOperator = partitions.iterator().next().getPartitionedInstance();

      while (morePartitionsToCreate-- > 0) {
        DefaultPartition<BlockWriter> partition = new DefaultPartition<BlockWriter>(anOperator);
        partitions.add(partition);
      }
    }

    //transfer the counters
    BlockWriter targetWriter = partitions.iterator().next().getPartitionedInstance();
    for (BasicCounters<MutableLong> removedCounter : deletedCounters) {
      addCounters(targetWriter.fileCounters, removedCounter);
    }
    LOG.debug("Block writers {}", partitions.size());
    return partitions;
  }

  /**
   * Transfers the counters in partitioning.
   *
   * @param target
   *          target counter
   * @param source
   *          removed counter
   */
  protected void addCounters(BasicCounters<MutableLong> target, BasicCounters<MutableLong> source)
  {
    for (Enum<BlockWriter.Counters> key : BlockWriter.Counters.values()) {
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

  /**
   * Directory under application directory where blocks gets stored
   * @return blocks directory
   */
  public String getBlocksDirectory()
  {
    return blocksDirectory;
  }

  /**
   * Directory under application directory where blocks gets stored
   * @param blocksDirectory blocks directory
   */
  public void setBlocksDirectory(String blocksDirectory)
  {
    this.blocksDirectory = blocksDirectory;
  }

  @Override
  public void partitioned(Map<Integer, Partition<BlockWriter>> partitions)
  {

  }

  private static final Logger LOG = LoggerFactory.getLogger(BlockWriter.class);

}
