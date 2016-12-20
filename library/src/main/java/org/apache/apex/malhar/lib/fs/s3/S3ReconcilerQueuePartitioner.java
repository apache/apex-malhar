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

package org.apache.apex.malhar.lib.fs.s3;

import java.util.List;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Operator;
import com.datatorrent.api.Stats;
import com.datatorrent.lib.partitioner.StatsAwareStatelessPartitioner;

/**
 * This partitioner looks at Reconciler queue size to decide no. of partitions.
 * This partitioner is used for S3Reconciler Operator.
 * @param <T>
 */
@org.apache.hadoop.classification.InterfaceStability.Evolving
public class S3ReconcilerQueuePartitioner<T extends Operator> extends StatsAwareStatelessPartitioner<T>
{
  private static final long serialVersionUID = -4407806429128758992L;

  private int maxPartitions = 16;
  private int minPartitions = 1;

  private int maxQueueSizePerPartition = 4;

  @Override
  protected int getLoad(BatchedOperatorStats stats)
  {
    double totalBacklog = 0;
    double statsPartitionCount = 0;
    for (Map.Entry<Integer, BatchedOperatorStats> partitionStatus : partitionedInstanceStatus.entrySet()) {
      BatchedOperatorStats batchedOperatorStats = partitionStatus.getValue();
      if (batchedOperatorStats != null) {
        List<Stats.OperatorStats> lastWindowedStats = batchedOperatorStats.getLastWindowedStats();
        if (lastWindowedStats != null && lastWindowedStats.size() > 0) {
          Stats.OperatorStats lastStats = lastWindowedStats.get(lastWindowedStats.size() - 1);
          Long queueLength = (Long)lastStats.metrics.get("queueLength");
          totalBacklog += queueLength;
          statsPartitionCount += 1;
          logger.debug("queueLength : {}, totalBacklog {},statsPartitionCount{}", queueLength, totalBacklog,
              statsPartitionCount);
        }
      }
    }

    double backlogPerPartition = totalBacklog / statsPartitionCount;
    logger.debug("backlogPerPartition : {}", backlogPerPartition);
    logger.debug("maxQueueSizePerPartition : {}, partitionedInstanceStatus.size():{}" + ", maxPartitions:{}",
        maxQueueSizePerPartition, partitionedInstanceStatus.size(), maxPartitions);

    if (backlogPerPartition > maxQueueSizePerPartition && partitionedInstanceStatus.size() < maxPartitions) {
      return 1;
    }
    logger.debug("minPartitions:{}", minPartitions);

    if (backlogPerPartition < 1.1 && partitionedInstanceStatus.size() > minPartitions) {
      return -1;
    }

    return 0;
  }

  public int getMaxPartitions()
  {
    return maxPartitions;
  }

  public void setMaxPartitions(int maxPartitions)
  {
    this.maxPartitions = maxPartitions;
  }

  public int getMinPartitions()
  {
    return minPartitions;
  }

  public void setMinPartitions(int minPartitions)
  {
    this.minPartitions = minPartitions;
  }

  public int getMaxQueueSizePerPartition()
  {
    return maxQueueSizePerPartition;
  }

  public void setMaxQueueSizePerPartition(int maxQueueSizePerPartition)
  {
    this.maxQueueSizePerPartition = maxQueueSizePerPartition;
  }

  private static final Logger logger = LoggerFactory.getLogger(S3ReconcilerQueuePartitioner.class);
}
