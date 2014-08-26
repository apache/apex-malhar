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

import com.datatorrent.api.Context;
import com.datatorrent.api.Context.CountersAggregator;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.Stats.OperatorStats;
import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.fs.Path;
import org.mortbay.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Input operator that reads files from a directory.
 * <p/>
 * Provides the same functionality as the AbstractFSDirectoryInputOperator
 * except that this utilized dynamic partitioning where the user can set the
 * preferred number of pending files per operator as well as the max number of
 * operators and define a repartition interval. When the repartition interval
 * passes then a new number of operators are created to accommodate the
 * remaining pending files.
 * <p/>
 * 
 * @since 1.0.4
 */
public abstract class AbstractThroughputHashFSDirectoryInputOperator<T> extends AbstractFSDirectoryInputOperator<T>
{
  private static final Logger LOG = LoggerFactory.getLogger(AbstractThroughputHashFSDirectoryInputOperator.class);

  private long repartitionInterval = 5L * 60L * 1000L;
  private int preferredMaxPendingFilesPerOperator = 10;
  private transient FileCount fileCounter;
  private transient OperatorContext context;
  
  public void setup(OperatorContext context)
  {
    super.setup(context);
    this.context = context;
  }
    
  public void setRepartitionInterval(long repartitionInterval)
  {
    this.repartitionInterval = repartitionInterval;
  }

  public long getRepartitionInterval()
  {
    return repartitionInterval;
  }

  public void setPreferredMaxPendingFilesPerOperator(int pendingFilesPerOperator)
  {
    this.preferredMaxPendingFilesPerOperator = pendingFilesPerOperator;
  }

  public int getPreferredMaxPendingFilesPerOperator()
  {
    return preferredMaxPendingFilesPerOperator;
  }

  @Override
  public void emitTuples()
  {
    if(System.currentTimeMillis() - scanIntervalMillis >= lastScanMillis) {
      Set<Path> newPaths = scanner.scan(fs, filePath, processedFiles);

      for(Path newPath : newPaths) {
        String newPathString = newPath.toString();
        pendingFiles.add(newPathString);
        processedFiles.add(newPathString);
      }
      lastScanMillis = System.currentTimeMillis();
    }

    super.emitTuples();
  }
  
  @Override
  public void endWindow()
  {
    super.endWindow();
    
    if(context != null)
    {
      fileCounter = new FileCount();
      
      fileCounter.fileCount = failedFiles.size() + 
                            pendingFiles.size() +
                            unfinishedFiles.size();
      context.setCounters(fileCounter);
    }
  }
  
  @Override
  protected int computedNewPartitionCount(Collection<Partition<AbstractFSDirectoryInputOperator<T>>> partitions, int incrementalCapacity)
  {
    LOG.debug("Called throughput.");
    boolean isInitialParitition = partitions.iterator().next().getStats() == null;
    int newOperatorCount;
    int totalFileCount = 0;
    
    for(Partition<AbstractFSDirectoryInputOperator<T>> partition : partitions) {
      AbstractFSDirectoryInputOperator<T> oper = partition.getPartitionedInstance();
      totalFileCount += oper.failedFiles.size();
      totalFileCount += oper.pendingFiles.size();
      totalFileCount += oper.unfinishedFiles.size();
      
      if (oper.currentFile != null) {
        totalFileCount++;
      }  
    }
    
    if(!isInitialParitition) {
      LOG.debug("definePartitions: Total File Count: {}", totalFileCount);
      newOperatorCount = computeOperatorCount(totalFileCount);
    }
    else {
      newOperatorCount = partitionCount;
    }
    
    return newOperatorCount;
  }
  
  private int computeOperatorCount(int totalFileCount)
  {
    int newOperatorCount = totalFileCount / preferredMaxPendingFilesPerOperator;

    if(totalFileCount % preferredMaxPendingFilesPerOperator > 0) {
      newOperatorCount++;
    }

    if(newOperatorCount > partitionCount) {
      newOperatorCount = partitionCount;
    }

    return newOperatorCount;
  }

  @Override
  public Response processStats(BatchedOperatorStats batchedOperatorStats)
  {
    Log.debug("Processing stats");
    int fileCount = 0;
    
    for(OperatorStats operatorStats: batchedOperatorStats.getLastWindowedStats())
    {
      if(operatorStats.counters != null)
      {
        fileCount = ((FileCount) operatorStats.counters).fileCount;
      }
    }
    
    Response response = new Response();
    
    fileCountMap.put(batchedOperatorStats.getOperatorId(), fileCount);
    
    LOG.debug("FileCounter {} {}", fileCountMap.keySet().size(), currentPartitions);
    
    if(fileCountMap.keySet().size() != currentPartitions)
    {
      response.repartitionRequired = false;
      return response;
    }
    
    fileCount = 0;
    
    Set<Integer> operatorIds = fileCountMap.keySet();
    
    for(Integer operatorId: operatorIds)
    {
      fileCount += fileCountMap.get(operatorId);
    }
    
    LOG.debug("FileCounter {}", fileCount);
    
    int newOperatorCount = computeOperatorCount(fileCount);
    
    if(newOperatorCount == currentPartitions ||
      System.currentTimeMillis() - repartitionInterval <= lastRepartition)
    {
      response.repartitionRequired = false;
      return response;
    }
    
    Log.debug("Repartition true.");
    response.repartitionRequired = true;
    return response;
  }
  
  static class FileCount implements Serializable
  {
    public int fileCount = 0;
  }
}
