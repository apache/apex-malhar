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

import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.junit.Assert;
import org.junit.Test;

import com.datatorrent.lib.partitioner.StatelessThroughputBasedPartitionerTest.DummyOperator;
import com.datatorrent.lib.util.TestUtils;

import com.datatorrent.api.*;

/**
 * Test for {@link StatelessLatencyBasedPartitioner}
 */
public class StatelessLatencyBasedPartitionerTest
{

  @Test
  public void testPartitioner() throws Exception
  {
    DummyOperator dummyOperator = new DummyOperator(5);

    StatelessLatencyBasedPartitioner<DummyOperator> statelessLatencyBasedPartitioner = new StatelessLatencyBasedPartitioner<DummyOperator>();
    statelessLatencyBasedPartitioner.setMaximumLatency(10);
    statelessLatencyBasedPartitioner.setMinimumLatency(1);
    statelessLatencyBasedPartitioner.setCooldownMillis(10);

    TestUtils.MockBatchedOperatorStats mockStats = new TestUtils.MockBatchedOperatorStats(2);
    mockStats.operatorStats = Lists.newArrayList();
    mockStats.latency = 11;

    List<Operator.InputPort<?>> ports = Lists.newArrayList();
    ports.add(dummyOperator.input);

    DefaultPartition<DummyOperator> defaultPartition = new DefaultPartition<DummyOperator>(dummyOperator);
    Collection<Partitioner.Partition<DummyOperator>> partitions = Lists.newArrayList();
    partitions.add(defaultPartition);
    partitions = statelessLatencyBasedPartitioner.definePartitions(partitions, new StatelessPartitionerTest.PartitioningContextImpl(ports, 1));
    Assert.assertTrue(1 == partitions.size());
    defaultPartition = (DefaultPartition<DummyOperator>) partitions.iterator().next();
    Map<Integer, Partitioner.Partition<DummyOperator>> partitionerMap = Maps.newHashMap();
    partitionerMap.put(2, defaultPartition);
    statelessLatencyBasedPartitioner.partitioned(partitionerMap);
    StatsListener.Response response = statelessLatencyBasedPartitioner.processStats(mockStats);
    Assert.assertEquals("repartition is false", false, response.repartitionRequired);
    Thread.sleep(100);
    response = statelessLatencyBasedPartitioner.processStats(mockStats);
    Assert.assertEquals("repartition is true", true, response.repartitionRequired);

    TestUtils.MockPartition<DummyOperator> mockPartition = new TestUtils.MockPartition<DummyOperator>(defaultPartition, mockStats);
    partitions.clear();
    partitions.add(mockPartition);

    Collection<Partitioner.Partition<DummyOperator>> newPartitions = statelessLatencyBasedPartitioner.definePartitions(partitions,
      new StatelessPartitionerTest.PartitioningContextImpl(ports, 5));
    Assert.assertEquals("after partition", 2, newPartitions.size());
  }
}
