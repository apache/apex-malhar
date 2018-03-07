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
package org.apache.apex.malhar.python.base.partitioner;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.python.base.BasePythonExecutionOperator;
import org.apache.apex.malhar.python.base.requestresponse.PythonRequestResponse;

/***
 * A partitioner that adds another instance of the python operator when the percent of non-serviced requests within any
 *  two checkpoint boundaries exceed a certain threshold. The threshold for triggering is configurable and is expressed
 *   as a percentage.
 */
public class ThreadStarvationBasedPartitioner extends AbstractPythonExecutionPartitioner
{
  private static final Logger LOG = LoggerFactory.getLogger(ThreadStarvationBasedPartitioner.class);

  private float threadStarvationThresholdRatio;

  public ThreadStarvationBasedPartitioner(BasePythonExecutionOperator prototypePythonOperator)
  {
    super(prototypePythonOperator);
  }

  /***
   * Calculates the partitions that are required based on the starvations encountered for each checkpoint state. The
   *  new instance is fed with the command history of the original operator ( if any ) so that the new instance can
   *   be in the same state of the original operator when it starts processing the new tuples.
   * @param partitions The current set of partitions
   * @param context The partitioning context
   * @return The new set of partitioned instances keeping the old ones in tact and rebuilding only new ones if needed.
   */
  @Override
  protected List<Partition<BasePythonExecutionOperator>> buildTargetPartitions(
      Collection<Partition<BasePythonExecutionOperator>> partitions, PartitioningContext context)
  {
    List<Partition<BasePythonExecutionOperator>> returnList = new ArrayList<>();
    if (partitions != null) {
      returnList.addAll(partitions);
      for (Partition<BasePythonExecutionOperator> aCurrentPartition : partitions) {
        BasePythonExecutionOperator anOperator = aCurrentPartition.getPartitionedInstance();
        long starvedCount = anOperator.getNumStarvedReturns();
        long requestsForCheckpointWindow = anOperator.getNumberOfRequestsProcessedPerCheckpoint();
        if ( requestsForCheckpointWindow != 0) { // when the operator is starting for the first time
          float starvationPercent = 100 - ( ((requestsForCheckpointWindow - starvedCount ) /
              requestsForCheckpointWindow) * 100);
          if (starvationPercent > anOperator.getStarvationPercentBeforeSpawningNewInstance()) {
            LOG.info("Creating a new instance of the python operator as starvation % is " + starvationPercent);
            Partition<BasePythonExecutionOperator> newInstance = clonePartition();
            List<PythonRequestResponse> commandHistory = new ArrayList<>();
            commandHistory.addAll(anOperator.getAccumulatedCommandHistory());
            newInstance.getPartitionedInstance().setAccumulatedCommandHistory(commandHistory);
            returnList.add(newInstance);
          }
        }
      }
    }
    return returnList;
  }

  public float getThreadStarvationThresholdRatio()
  {
    return threadStarvationThresholdRatio;
  }

  public void setThreadStarvationThresholdRatio(float threadStarvationThresholdRatio)
  {
    this.threadStarvationThresholdRatio = threadStarvationThresholdRatio;
  }
}
