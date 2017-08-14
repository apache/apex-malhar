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
package org.apache.apex.malhar.lib.bandwidth;

import java.util.Collection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.common.partitioner.StatelessPartitioner;

/**
 * @since 3.5.0
 */
public class BandwidthPartitioner<T extends BandwidthLimitingOperator> extends StatelessPartitioner<T>
{
  private static final long serialVersionUID = -7502505996637650237L;
  private static final Logger LOG = LoggerFactory.getLogger(BandwidthPartitioner.class);

  /**
   * This creates a partitioner which creates only one partition.
   */
  public BandwidthPartitioner()
  {
  }

  /**
   * This constructor is used to create the partitioner from a property.
   *
   * @param value A string which is an integer of the number of partitions to create
   */
  public BandwidthPartitioner(String value)
  {
    super(value);
  }

  /**
   * This creates a partitioner which creates partitonCount partitions.
   *
   * @param partitionCount The number of partitions to create.
   */
  public BandwidthPartitioner(int partitionCount)
  {
    super(partitionCount);
  }

  @Override
  public Collection<Partition<T>> definePartitions(Collection<Partition<T>> partitions, PartitioningContext context)
  {
    long currentBandwidth = partitions.iterator().next().getPartitionedInstance().getBandwidthManager().getBandwidth()
        * partitions.size();
    Collection<Partition<T>> newpartitions = super.definePartitions(partitions, context);
    return updateBandwidth(newpartitions, currentBandwidth);
  }

  public Collection<Partition<T>> updateBandwidth(Collection<Partition<T>> newpartitions, long currentBandwidth)
  {
    long newBandwidth = currentBandwidth / newpartitions.size();
    for (Partition<T> partition : newpartitions) {
      partition.getPartitionedInstance().getBandwidthManager().setBandwidth(newBandwidth);
    }
    LOG.info("Updating bandwidth of partitions to value: " + newBandwidth);
    return newpartitions;
  }
}
