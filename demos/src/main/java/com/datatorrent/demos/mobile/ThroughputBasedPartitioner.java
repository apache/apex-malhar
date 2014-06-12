package com.datatorrent.demos.mobile;

import java.io.Serializable;
import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;
import com.sun.org.apache.xalan.internal.xsltc.runtime.Operators;

import com.datatorrent.api.DefaultPartition;
import com.datatorrent.api.Operator;
import com.datatorrent.api.Partitioner;
import com.datatorrent.api.StatsListener;

/**
 * Created by gaurav on 6/11/14.
 */
public class ThroughputBasedPartitioner<T extends Operator> implements StatsListener, Partitioner<T>, Serializable
{
  private long maximumEvents;
  private long minimumEvents;
  private long coolDownMillis = 2000;
  private long nextMillis;
  private long partitionNextMillis;

  @Override
  public Response processStats(BatchedOperatorStats stats)
  {
    Response response = new Response();
    response.repartitionRequired = false;
    final HashMap<Integer, BatchedOperatorStats> map = partitionedInstanceStatus.get();
    if (!map.containsKey(stats.getOperatorId())) {
      return response;
    }
    map.put(stats.getOperatorId(), stats);
    if (System.currentTimeMillis() > nextMillis) {
      if (stats.getTuplesProcessedPSMA() < minimumEvents || stats.getTuplesProcessedPSMA() > maximumEvents) {
        response.repartitionRequired = true;
        logger.debug("setting repartition to true");
      }
      nextMillis = System.currentTimeMillis() + coolDownMillis;
    }
    return response;
  }

  @Override
  public Collection<Partition<T>> definePartitions(Collection<Partition<T>> partitions, int incrementalCapacity)
  {
    if (partitionedInstanceStatus.get().isEmpty()) {
      // first call
      // trying to give initial stability before sending the repartition call
      partitionNextMillis = System.currentTimeMillis() + 2 * coolDownMillis;
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
      partitionNextMillis = System.currentTimeMillis() + coolDownMillis;
      return newPartitions;

    }
  }

  @Override
  public void partitioned(Map<Integer, Partition<T>> partitions)
  {
    logger.debug("Partitioned Map: {}", partitions);
    HashMap<Integer, BatchedOperatorStats> map = partitionedInstanceStatus.get();
    map.clear();
    for (Map.Entry<Integer, Partition<T>> entry : partitions.entrySet()) {
      if (map.containsKey(entry.getKey())) {
      }
      else {
        map.put(entry.getKey(), null);
      }
    }
  }

  public void setMaximumThroughput(long maxEvents)
  {
    maximumEvents = maxEvents;
  }

  public void setMinimumThroughput(long minEvents)
  {
    minimumEvents = minEvents;
  }

  public void setCoolDownMillis(long millis)
  {
    coolDownMillis = millis;
  }

  private static final transient ThreadLocal<HashMap<Integer, BatchedOperatorStats>> partitionedInstanceStatus = new ThreadLocal<HashMap<Integer, BatchedOperatorStats>>()
  {
    @Override
    protected HashMap<Integer, BatchedOperatorStats> initialValue()
    {
      return new HashMap<Integer, BatchedOperatorStats>();
    }

  };

  private static final Logger logger = LoggerFactory.getLogger(ThroughputBasedPartitioner.class);
}
