/**
 * Copyright (C) 2015 DataTorrent, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.partitioner;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.*;

import javax.validation.constraints.Min;

import com.google.common.collect.Sets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.DefaultPartition;
import com.datatorrent.api.Operator;
import com.datatorrent.api.Partitioner;
import com.datatorrent.api.StatsListener;

import com.datatorrent.common.partitioner.StatelessPartitioner;

/**
 * <p>
 * This does the partition of the operator based on the load. This partitioner doesn't copy the state during the repartitioning
 * Concrete classes should implement getLoad() function that calculates the load on the operator
 * </p>
 *
 * <b>Properties</b>:<br>
 * <b>cooldownMillis:</b> The time for the operators to stabilize before next partitioning if required<br>
 * <br>
 *
 * @param <T> Operator type
 *
 * @since 2.1.0
 */
public abstract class StatsAwareStatelessPartitioner<T extends Operator> implements StatsListener, Partitioner<T>, Serializable
{
  private static final Logger logger = LoggerFactory.getLogger(StatsAwareStatelessPartitioner.class);
  private static final long serialVersionUID = 201504021522L;

  private long cooldownMillis = 2000;
  private long nextMillis;
  private long partitionNextMillis;
  private boolean repartition;
  private transient HashMap<Integer, BatchedOperatorStats> partitionedInstanceStatus = new HashMap<Integer, BatchedOperatorStats>();
  @Min(1)
  private int initialPartitionCount = 1;

  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException
  {
    in.defaultReadObject();
    this.partitionedInstanceStatus = new HashMap<Integer, BatchedOperatorStats>();
  }

  /**
   * This creates a partitioner which begins with only one partition.
   */
  public StatsAwareStatelessPartitioner()
  {
  }

  /**
   * This constructor is used to create the partitioner from a property.
   *
   * @param value A string which is an integer of the number of partitions to begin with
   */
  public StatsAwareStatelessPartitioner(String value)
  {
    this(Integer.parseInt(value));
  }

  /**
   * This creates a partitioner which creates partitonCount partitions.
   *
   * @param initialPartitionCount The number of partitions to begin with.
   */
  public StatsAwareStatelessPartitioner(int initialPartitionCount)
  {
    this.initialPartitionCount = initialPartitionCount;
  }

  public void setInitialPartitionCount(int initialPartitionCount)
  {
    this.initialPartitionCount = initialPartitionCount;
  }

  public int getInitialPartitionCount()
  {
    return initialPartitionCount;
  }

  @Override
  public Response processStats(BatchedOperatorStats stats)
  {
    Response response = new Response();
    response.repartitionRequired = false;
    if (!partitionedInstanceStatus.containsKey(stats.getOperatorId())) {
      return response;
    }
    partitionedInstanceStatus.put(stats.getOperatorId(), stats);
    if (getLoad(stats) != 0) {
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
  public Collection<Partition<T>> definePartitions(Collection<Partition<T>> partitions, PartitioningContext context)
  {
    if (partitionedInstanceStatus == null || partitionedInstanceStatus.isEmpty()) {
      // first call
      // trying to give initial stability before sending the repartition call
      if (partitionedInstanceStatus == null) {
        partitionedInstanceStatus = new HashMap<Integer, BatchedOperatorStats>();
      }
      partitionNextMillis = System.currentTimeMillis() + 2 * cooldownMillis;
      nextMillis = partitionNextMillis;
      // delegate to create initial list of partitions
      return new StatelessPartitioner<T>(initialPartitionCount).definePartitions(partitions, context);
    }
    else {
      // repartition call
      logger.debug("repartition call for operator");
      if (System.currentTimeMillis() < partitionNextMillis) {
        return partitions;
      }
      List<Partition<T>> newPartitions = new ArrayList<Partition<T>>();
      HashMap<Integer, Partition<T>> lowLoadPartitions = new HashMap<Integer, Partition<T>>();
      for (Partition<T> p : partitions) {
        int load = getLoad(p.getStats());
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

  /**
   * This checks the load on the operator
   *
   * @param stats
   * @return if operator is overloaded then it should return 1, if operator is underloaded then it should return -1 else 0
   */
  protected abstract int getLoad(BatchedOperatorStats stats);

  public long getCooldownMillis()
  {
    return cooldownMillis;
  }

  public void setCooldownMillis(long cooldownMillis)
  {
    this.cooldownMillis = cooldownMillis;
  }
}
