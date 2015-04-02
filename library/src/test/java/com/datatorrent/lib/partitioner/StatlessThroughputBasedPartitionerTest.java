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

import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import com.datatorrent.api.DefaultPartition;
import com.datatorrent.api.Operator;
import com.datatorrent.api.Partitioner;
import com.datatorrent.api.StatsListener;

import com.datatorrent.lib.util.TestUtils;

public class StatlessThroughputBasedPartitionerTest
{
  public static class TestStats extends TestUtils.MockBatchedOperatorStats
  {

    public TestStats(int operatorId)
    {
      super(operatorId);
    }

    public long tuplesProcessedPSMA;

    @Override
    public long getTuplesProcessedPSMA()
    {
      return tuplesProcessedPSMA;
    }
  }

  @Test
  public void testPartitioner() throws Exception
  {
    StatelessLatencyBasedPartitionerTest.DummyOperator dummyOperator = new StatelessLatencyBasedPartitionerTest.DummyOperator(5);

    StatelessThroughputBasedPartitioner<StatelessLatencyBasedPartitionerTest.DummyOperator> statelessLatencyBasedPartitioner = new StatelessThroughputBasedPartitioner<StatelessLatencyBasedPartitionerTest.DummyOperator>();
    statelessLatencyBasedPartitioner.setMaximumEvents(10);
    statelessLatencyBasedPartitioner.setMinimumEvents(1);
    statelessLatencyBasedPartitioner.setCooldownMillis(10);

    TestStats mockStats = new TestStats(2);
    mockStats.operatorStats = Lists.newArrayList();
    mockStats.tuplesProcessedPSMA = 11;

    List<Operator.InputPort<?>> ports = Lists.newArrayList();
    ports.add(dummyOperator.input);

    DefaultPartition<StatelessLatencyBasedPartitionerTest.DummyOperator> defaultPartition = new DefaultPartition<StatelessLatencyBasedPartitionerTest.DummyOperator>(dummyOperator);
    Collection<Partitioner.Partition<StatelessLatencyBasedPartitionerTest.DummyOperator>> partitions = Lists.newArrayList();
    partitions.add(defaultPartition);
    partitions = statelessLatencyBasedPartitioner.definePartitions(partitions, new StatelessPartitionerTest.PartitioningContextImpl(ports, 1));
    Assert.assertTrue(1 == partitions.size());
    defaultPartition = (DefaultPartition<StatelessLatencyBasedPartitionerTest.DummyOperator>) partitions.iterator().next();
    Map<Integer, Partitioner.Partition<StatelessLatencyBasedPartitionerTest.DummyOperator>> partitionerMap = Maps.newHashMap();
    partitionerMap.put(2, defaultPartition);
    statelessLatencyBasedPartitioner.partitioned(partitionerMap);
    StatsListener.Response response = statelessLatencyBasedPartitioner.processStats(mockStats);
    Assert.assertEquals("repartition is false", false, response.repartitionRequired);
    Thread.sleep(15);
    response = statelessLatencyBasedPartitioner.processStats(mockStats);
    Assert.assertEquals("repartition is true", true, response.repartitionRequired);

    TestUtils.MockPartition<StatelessLatencyBasedPartitionerTest.DummyOperator> mockPartition = new TestUtils.MockPartition<StatelessLatencyBasedPartitionerTest.DummyOperator>(defaultPartition, mockStats);
    partitions.clear();
    partitions.add(mockPartition);

    Collection<Partitioner.Partition<StatelessLatencyBasedPartitionerTest.DummyOperator>> newPartitions = statelessLatencyBasedPartitioner.definePartitions(partitions,
      new StatelessPartitionerTest.PartitioningContextImpl(ports, 5));
    Assert.assertEquals("after partition", 2, newPartitions.size());
  }
}
