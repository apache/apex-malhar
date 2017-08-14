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
package org.apache.apex.malhar.lib.partitioner;

import com.datatorrent.api.Operator;

/**
 * <p>
 * This does the partition of the operator based on the throughput. This partitioner doesn't copy the state during the repartitioning
 * The max and min throughput can be controlled by the properties
 * </p>
 *
 *
 * <b>Properties</b>:<br>
 * <b>maximumEvents:</b> The maximum throughput above which the operator will be repartitioned<br>
 * <b>minimumEvents:</b> The minimum throughput below which the operators will be merged<br>
 * <br>
 *
 * @param <T> Operator type
 * @since 1.0.2
 */
public class StatelessThroughputBasedPartitioner<T extends Operator> extends StatsAwareStatelessPartitioner<T>
{
  private static final long serialVersionUID = 201412021109L;

  private long maximumEvents;
  private long minimumEvents;

  @Override
  protected int getLoad(BatchedOperatorStats stats)
  {
    if (stats.getTuplesProcessedPSMA() > maximumEvents) {
      return 1;
    }
    if (stats.getTuplesProcessedPSMA() < minimumEvents) {
      return -1;
    }
    return 0;
  }

  /**
   * This creates a partitioner which begins with only one partition.
   */
  public StatelessThroughputBasedPartitioner()
  {
    super();
  }

  /**
   * This constructor is used to create the partitioner from a property.
   *
   * @param value A string which is an integer of the number of partitions to begin with
   */
  public StatelessThroughputBasedPartitioner(String value)
  {
    super(value);
  }

  /**
   * This creates a partitioner which creates partitonCount partitions.
   *
   * @param initialPartitionCount The number of partitions to begin with.
   */
  public StatelessThroughputBasedPartitioner(int initialPartitionCount)
  {
    super(initialPartitionCount);
  }

  public long getMaximumEvents()
  {
    return maximumEvents;
  }

  public void setMaximumEvents(long maximumEvents)
  {
    this.maximumEvents = maximumEvents;
  }

  public long getMinimumEvents()
  {
    return minimumEvents;
  }

  public void setMinimumEvents(long minimumEvents)
  {
    this.minimumEvents = minimumEvents;
  }

}
