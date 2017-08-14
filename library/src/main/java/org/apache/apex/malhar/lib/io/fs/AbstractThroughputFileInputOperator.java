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

import java.util.Collection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.counters.BasicCounters;
import org.apache.commons.lang.mutable.MutableLong;

import com.datatorrent.api.Stats.OperatorStats;

/**
 * This is the base implementation for a file input operator, which scans a directory for files.&nbsp;
 * Files are then read and split into tuples, which are emitted.&nbsp;
 * This operator is partitioned base on the number of files which remain to be processed.&nbsp;
 * A concrete operator should be created from this skeleton implementation.
 * <p>
 * Provides the same functionality as the AbstractFSDirectoryInputOperator
 * except that this utilized dynamic partitioning where the user can set the
 * preferred number of pending files per operator as well as the max number of
 * operators and define a repartition interval. If a physical operator runs out
 * of files to process and an amount of time greater than or equal to the
 * repartition interval has passed then a new number of operators are created
 * to accommodate the remaining pending files.
 * </p>
 *
 * @displayName FS Throughput Directory Scan Input
 * @category Input
 * @tags hdfs, directory, input operator
 *
 * @since 1.0.4
 */
@org.apache.hadoop.classification.InterfaceStability.Evolving
public abstract class AbstractThroughputFileInputOperator<T> extends AbstractFileInputOperator<T>
{
  private static final Logger LOG = LoggerFactory.getLogger(AbstractThroughputFileInputOperator.class);

  private long repartitionInterval = 5L * 60L * 1000L;
  private int preferredMaxPendingFilesPerOperator = 10;

  /**
   * Sets the minimum amount of time that must pass in milliseconds before the
   * operator can be repartitioned.
   * @param repartitionInterval The minimum amount of time that must pass in
   * milliseconds before the operator can be repartitioned.
   */
  public void setRepartitionInterval(long repartitionInterval)
  {
    this.repartitionInterval = repartitionInterval;
  }

  /**
   * Gets the minimum amount of time that must pass in milliseconds before the
   * operator can be repartitioned.
   * @return The minimum amount of time that must pass in milliseconds before
   * the operator can be repartitioned.
   */
  public long getRepartitionInterval()
  {
    return repartitionInterval;
  }

  /**
   * Sets the preferred number of pending files per operator.
   * @param pendingFilesPerOperator The preferred number of pending files
   * per operator.
   */
  public void setPreferredMaxPendingFilesPerOperator(int pendingFilesPerOperator)
  {
    this.preferredMaxPendingFilesPerOperator = pendingFilesPerOperator;
  }

  /**
   * Returns the preferred number of pending files per operator.
   * @return The preferred number of pending files per operator.
   */
  public int getPreferredMaxPendingFilesPerOperator()
  {
    return preferredMaxPendingFilesPerOperator;
  }

  /**
   * Returns the maximum number of partitions for the operator.
   * @return The maximum number of partitions for the operator.
   */
  @Override
  public int getPartitionCount()
  {
    return super.getPartitionCount();
  }

  /**
   * Sets the maximum number of partitions for the operator.
   * @param requiredPartitions The maximum number of partitions for the
   * operator.
   */
  @Override
  public void setPartitionCount(int requiredPartitions)
  {
    super.setPartitionCount(requiredPartitions);
  }

  @Override
  public void emitTuples()
  {
    scanDirectory();

    super.emitTuples();
  }

  @Override
  protected int getNewPartitionCount(Collection<Partition<AbstractFileInputOperator<T>>> partitions, PartitioningContext context)
  {
    LOG.debug("Called throughput.");
    boolean isInitialParitition = partitions.iterator().next().getStats() == null;
    int newOperatorCount;
    int totalFileCount = 0;

    for (Partition<AbstractFileInputOperator<T>> partition : partitions) {
      AbstractFileInputOperator<T> oper = partition.getPartitionedInstance();
      totalFileCount += oper.failedFiles.size();
      totalFileCount += oper.pendingFiles.size();
      totalFileCount += oper.unfinishedFiles.size();

      if (oper.currentFile != null) {
        totalFileCount++;
      }
    }

    if (!isInitialParitition) {
      LOG.debug("definePartitions: Total File Count: {}", totalFileCount);
      newOperatorCount = computeOperatorCount(totalFileCount);
    } else {
      newOperatorCount = partitionCount;
    }

    return newOperatorCount;
  }

  private int computeOperatorCount(int totalFileCount)
  {
    int newOperatorCount = totalFileCount / preferredMaxPendingFilesPerOperator;

    if (totalFileCount % preferredMaxPendingFilesPerOperator > 0) {
      newOperatorCount++;
    }
    if (newOperatorCount > partitionCount) {
      newOperatorCount = partitionCount;
    }
    if (newOperatorCount == 0) {
      newOperatorCount = 1;
    }

    return newOperatorCount;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Response processStats(BatchedOperatorStats batchedOperatorStats)
  {
    BasicCounters<MutableLong> fileCounters = null;

    for (OperatorStats operatorStats : batchedOperatorStats.getLastWindowedStats()) {
      if (operatorStats.counters != null) {
        fileCounters = (BasicCounters<MutableLong>)operatorStats.counters;
      }
    }

    Response response = new Response();

    if (fileCounters != null &&
        fileCounters.getCounter(FileCounters.PENDING_FILES).longValue() > 0L ||
        System.currentTimeMillis() - repartitionInterval <= lastRepartition) {
      response.repartitionRequired = false;
      return response;
    }

    response.repartitionRequired = true;
    return response;
  }
}
