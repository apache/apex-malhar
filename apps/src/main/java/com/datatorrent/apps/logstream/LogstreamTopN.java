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
package com.datatorrent.apps.logstream;

import java.util.*;
import java.util.Map.Entry;

import javax.validation.constraints.NotNull;

import com.google.common.collect.Sets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.lib.algo.TopN;
import com.datatorrent.lib.codec.KryoSerializableStreamCodec;
import com.datatorrent.lib.logs.DimensionObject;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultPartition;
import com.datatorrent.api.Partitioner;
import com.datatorrent.api.StreamCodec;

import com.datatorrent.apps.logstream.PropertyRegistry.LogstreamPropertyRegistry;
import com.datatorrent.apps.logstream.PropertyRegistry.PropertyRegistry;
import com.datatorrent.common.util.DTThrowable;

/**
 * Partitionable topN operator.
 * Each partition serves specific filter as defined in the partition.
 *
 */
public class LogstreamTopN extends TopN<String, DimensionObject<String>> implements Partitioner<LogstreamTopN>
{
  private transient boolean firstTuple = true;
  private final HashMap<String, Number> recordType = new HashMap<String, Number>();
  private static final Logger logger = LoggerFactory.getLogger(LogstreamTopN.class);
  @NotNull
  private PropertyRegistry<String> registry;

  /**
   * supply the registry object which is used to store and retrieve meta information about each tuple
   *
   * @param registry
   */
  public void setRegistry(PropertyRegistry<String> registry)
  {
    this.registry = registry;
  }

  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);
    LogstreamPropertyRegistry.setInstance(registry);
  }

  @Override
  public void processTuple(Map<String, DimensionObject<String>> tuple)
  {
    if (firstTuple) {
      extractType(tuple);
      firstTuple = false;
    }

    Iterator<Entry<String, DimensionObject<String>>> iterator = tuple.entrySet().iterator();
    String randomKey = null;

    if (iterator.hasNext()) {
      randomKey = iterator.next().getKey();
    }

    // what happens if randomKey is null?
    String[] split = randomKey.split("\\|");
    Number receivedFilter = new Integer(split[3]);
    Number expectedFilter = recordType.get(LogstreamUtil.FILTER);

    if (!receivedFilter.equals(expectedFilter)) {
      logger.error("Unexpected tuple");
      logger.error("expected filter = {} received = {}", expectedFilter, receivedFilter);
    }

    super.processTuple(tuple);
  }

  @Override
  protected Class<? extends StreamCodec<Map<String, DimensionObject<String>>>> getStreamCodec()
  {
    return LogstreamTopNStreamCodec.class;
  }

  @Override
  public Collection<Partition<LogstreamTopN>> definePartitions(Collection<Partition<LogstreamTopN>> partitions, int incrementalCapacity)
  {
    ArrayList<Partition<LogstreamTopN>> newPartitions = new ArrayList<Partition<LogstreamTopN>>();
    String[] filters = registry.list(LogstreamUtil.FILTER);
    int partitionSize;

    if (partitions.size() == 1) {
      // initial partitions; functional partitioning
      partitionSize = filters.length;
    }
    else {
      // redo partitions; double the partitions
      partitionSize = partitions.size() * 2;

    }
    for (int i = 0; i < partitionSize; i++) {
      try {
        LogstreamTopN logstreamTopN = new LogstreamTopN();
        logstreamTopN.registry = this.registry;
        logstreamTopN.setN(this.getN());

        Partition<LogstreamTopN> partition = new DefaultPartition<LogstreamTopN>(logstreamTopN);
        newPartitions.add(partition);
      }
      catch (Throwable ex) {
        DTThrowable.rethrow(ex);
      }
    }

    int partitionBits = (Integer.numberOfLeadingZeros(0) - Integer.numberOfLeadingZeros(partitionSize / filters.length - 1));
    int partitionMask = 0;
    if (partitionBits > 0) {
      partitionMask = -1 >>> (Integer.numberOfLeadingZeros(-1)) - partitionBits;
    }

    partitionMask = (partitionMask << 16) | 0xffff; // right most 16 bits used for functional partitioning

    for (int i = 0; i < newPartitions.size(); i++) {
      Partition<LogstreamTopN> partition = newPartitions.get(i);
      String partitionVal = filters[i % filters.length];
      int bits = i / filters.length;
      int filterId = registry.getIndex(LogstreamUtil.FILTER, partitionVal);
      filterId = 0xffff & filterId; // clear out first 16 bits
      int partitionKey = (bits << 16) | filterId; // first 16 bits for dynamic partitioning, last 16 bits for functional partitioning
      logger.debug("partitionKey = {} partitionMask = {}", Integer.toBinaryString(partitionKey), Integer.toBinaryString(partitionMask));
      partition.getPartitionKeys().put(data, new PartitionKeys(partitionMask, Sets.newHashSet(partitionKey)));
    }

    return newPartitions;

  }

  @Override
  public void partitioned(Map<Integer, Partition<LogstreamTopN>> partitions)
  {
  }

  /**
   * extracts the meta information about the tuple
   *
   * @param tuple
   */
  private void extractType(Map<String, DimensionObject<String>> tuple)
  {
    Iterator<Entry<String, DimensionObject<String>>> iterator = tuple.entrySet().iterator();
    String randomKey = null;

    if (iterator.hasNext()) {
      randomKey = iterator.next().getKey();
    }
    String[] split = randomKey.split("\\|");
    Number filterId = new Integer(split[3]);

    recordType.put(LogstreamUtil.FILTER, filterId);

  }

  public static class LogstreamTopNStreamCodec extends KryoSerializableStreamCodec<Map<String, DimensionObject<String>>>
  {
    @Override
    public int getPartition(Map<String, DimensionObject<String>> t)
    {
      Iterator<String> iterator = t.keySet().iterator();
      String key = iterator.next();

      String[] split = key.split("\\|");
      int filterId = new Integer(split[3]); // filter id location in input record key
      int ret = 0;
      int hashCode = t.hashCode();

      filterId = 0xffff & filterId; // clear out first 16 bits
      ret = (hashCode << 16) | filterId; // first 16 bits represent hashcode, last 16 bits represent filter type

      return ret;
    }

  }

}
