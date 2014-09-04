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
package com.datatorrent.demos.mobile;

import java.io.Serializable;
import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

import com.datatorrent.api.DefaultPartition;
import com.datatorrent.api.Operator;
import com.datatorrent.api.Partitioner;
import com.datatorrent.api.StatsListener;

/**
 * This does the partition of the operator based on the throughput.
 * The max and min throughput can be controlled by the properties
 *
 * </br>
 * <b>maximumEvents:</b> The maximum throughput above which the operator will be repartitioned</br>
 * <b>minimumEvents:</b> The minimum throughput below which the operators will be merged</br>
 * <b>cooldownMillis:</b> The time for the operators to stabilize before next partitioning if required</br>
 * <b></b></br>
 *
 * @since 1.0.2
 */
public class ThroughputBasedPartitioner<T extends Operator> implements StatsListener, Partitioner<T>, Serializable
{
  private long maximumEvents;
  private long minimumEvents;
  private long cooldownMillis = 2000;
  private long nextMillis;
  private long partitionNextMillis;
  private boolean repartition;

  @Override
  public Response processStats(BatchedOperatorStats stats)
  {
    Response response = new Response();
    response.repartitionRequired = false;
    if (!partitionedInstanceStatus.containsKey(stats.getOperatorId())) {
      return response;
    }
    partitionedInstanceStatus.put(stats.getOperatorId(), stats);
    if (stats.getTuplesProcessedPSMA() < minimumEvents || stats.getTuplesProcessedPSMA() > maximumEvents) {
      if (repartition && System.currentTimeMillis() > nextMillis) {
        repartition = false;
        response.repartitionRequired = true;
        logger.debug("setting repartition to true");

      }
      else if (!repartition) {
        repartition = true;
        nextMillis = System.currentTimeMillis() + cooldownMillis;
      }
    }
    else {
      repartition = false;
    }
    return response;
  }

  @Override
  public Collection<Partition<T>> definePartitions(Collection<Partition<T>> partitions, int incrementalCapacity)
  {
    if (partitionedInstanceStatus == null || partitionedInstanceStatus.isEmpty()) {
      // first call
      // trying to give initial stability before sending the repartition call
      if (partitionedInstanceStatus == null) {
        partitionedInstanceStatus = new HashMap<Integer, BatchedOperatorStats>();
      }
      partitionNextMillis = System.currentTimeMillis() + 2 * cooldownMillis;
      nextMillis = partitionNextMillis;
      return null;
    }
    else {
      // repartition call
      logger.debug("repartition call for phone movement operator");
      if (System.currentTimeMillis() < partitionNextMillis) {
        return partitions;
      }
      BatchedOperatorStats stats;
      List<Partition<T>> newPartitions = new ArrayList<Partition<T>>();
      HashMap<Integer, Partition<T>> lowLoadPartitions = new HashMap<Integer, Partition<T>>();
      for (Partition<T> p : partitions) {
        int load = 0;
        stats = p.getStats();
        if (stats.getTuplesProcessedPSMA() > maximumEvents) {
          load = 1;
        }
        else if (stats.getTuplesProcessedPSMA() < minimumEvents) {
          load = -1;
        }
        if (load < 0) {
          // combine neighboring underutilized partitions
          PartitionKeys pks = p.getPartitionKeys().values().iterator().next(); // one port partitioned
          for (int partitionKey : pks.partitions) {
            // look for the sibling partition by excluding leading bit
            int reducedMask = pks.mask >>> 1;
            String lookupKey = Integer.valueOf(reducedMask) + "-" + Integer.valueOf(partitionKey & reducedMask);
            logger.debug("pks {} lookupKey {}", pks, lookupKey);
            Partition<T> siblingPartition = lowLoadPartitions.remove(partitionKey & reducedMask);
            if (siblingPartition == null) {
              lowLoadPartitions.put(partitionKey & reducedMask, p);
            }
            else {
              // both of the partitions are low load, combine
              PartitionKeys newPks = new PartitionKeys(reducedMask, Sets.newHashSet(partitionKey & reducedMask));
              // put new value so the map gets marked as modified
              Operator.InputPort<?> port = siblingPartition.getPartitionKeys().keySet().iterator().next();
              siblingPartition.getPartitionKeys().put(port, newPks);
              // add as new partition
              newPartitions.add(siblingPartition);
              //LOG.debug("partition keys after merge {}", siblingPartition.getPartitionKeys());
            }
          }
        }
        else if (load > 0) {
          // split bottlenecks
          Map<Operator.InputPort<?>, PartitionKeys> keys = p.getPartitionKeys();
          Map.Entry<Operator.InputPort<?>, PartitionKeys> e = keys.entrySet().iterator().next();

          final int newMask;
          final Set<Integer> newKeys;

          if (e.getValue().partitions.size() == 1) {
            // split single key
            newMask = (e.getValue().mask << 1) | 1;
            int key = e.getValue().partitions.iterator().next();
            int key2 = (newMask ^ e.getValue().mask) | key;
            newKeys = Sets.newHashSet(key, key2);
          }
          else {
            // assign keys to separate partitions
            newMask = e.getValue().mask;
            newKeys = e.getValue().partitions;
          }

          for (int key : newKeys) {
            Partition<T> newPartition = new DefaultPartition<T>(p.getPartitionedInstance());
            newPartition.getPartitionKeys().put(e.getKey(), new PartitionKeys(newMask, Sets.newHashSet(key)));
            newPartitions.add(newPartition);
          }
        }
        else {
          // leave unchanged
          newPartitions.add(p);
        }
      }
      // put back low load partitions that could not be combined
      newPartitions.addAll(lowLoadPartitions.values());
      partitionNextMillis = System.currentTimeMillis() + cooldownMillis;
      return newPartitions;

    }
  }

  @Override
  public void partitioned(Map<Integer, Partition<T>> partitions)
  {
    logger.debug("Partitioned Map: {}", partitions);
    partitionedInstanceStatus.clear();
    for (Map.Entry<Integer, Partition<T>> entry : partitions.entrySet()) {
      if (partitionedInstanceStatus.containsKey(entry.getKey())) {
      }
      else {
        partitionedInstanceStatus.put(entry.getKey(), null);
      }
    }
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

  public long getCooldownMillis()
  {
    return cooldownMillis;
  }

  public void setCooldownMillis(long cooldownMillis)
  {
    this.cooldownMillis = cooldownMillis;
  }

  private transient HashMap<Integer, BatchedOperatorStats> partitionedInstanceStatus = new HashMap<Integer, BatchedOperatorStats>();

  private static final Logger logger = LoggerFactory.getLogger(ThroughputBasedPartitioner.class);
}
