/*
 * Copyright (c) 2015 DataTorrent, Inc. ALL Rights Reserved.
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
package com.datatorrent.lib.partitioner;

import com.datatorrent.api.Operator;

/**
 * <p>
 * This does the partition of the operator based on the latency. This partitioner doesn't copy the state during the repartitioning
 * The max and min latency can be controlled by the properties
 * </p>
 *
 *
 * <b>Properties</b>:<br>
 * <b>maximumLatency:</b> The maximum Latency above which the operator will be repartitioned<br>
 * <b>minimumLatency:</b> The minimum Latency below which the operators will be merged<br>
 * <br>
 *
 * @param <T> Operator type
 *
 * @since 2.1.0
 */
public class StatelessLatencyBasedPartitioner<T extends Operator> extends StatsAwareStatelessPartitioner<T>
{
  private static final long serialVersionUID = 201503310930L;

  private long maximumLatency;
  private long minimumLatency;

  /**
   * This creates a partitioner which begins with only one partition.
   */
  public StatelessLatencyBasedPartitioner()
  {
    super();
  }

  /**
   * This constructor is used to create the partitioner from a property.
   *
   * @param value A string which is an integer of the number of partitions to begin with
   */
  public StatelessLatencyBasedPartitioner(String value)
  {
    super(value);
  }

  /**
   * This creates a partitioner which creates partitonCount partitions.
   *
   * @param initialPartitionCount The number of partitions to begin with.
   */
  public StatelessLatencyBasedPartitioner(int initialPartitionCount)
  {
    super(initialPartitionCount);
  }

  public long getMaximumLatency()
  {
    return maximumLatency;
  }

  public void setMaximumLatency(long maximumLatency)
  {
    this.maximumLatency = maximumLatency;
  }

  public long getMinimumLatency()
  {
    return minimumLatency;
  }

  public void setMinimumLatency(long minimumLatency)
  {
    this.minimumLatency = minimumLatency;
  }

  @Override
  protected int getLoad(BatchedOperatorStats stats)
  {
    if (stats.getLatencyMA() > maximumLatency) {
      return 1;
    }
    if (stats.getLatencyMA() < minimumLatency) {
      return -1;
    }
    return 0;
  }
}
