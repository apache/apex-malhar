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
import java.util.Iterator;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.google.common.collect.Lists;

import com.datatorrent.api.DefaultPartition;
import com.datatorrent.api.Partitioner;
import com.datatorrent.api.Partitioner.Partition;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class BandwidthPartitionerTest
{
  @Mock
  private BandwidthManager bandwidthManagerMock;
  @Mock
  private BandwidthLimitingOperator operatorMock;
  @Mock
  private Partition<BandwidthLimitingOperator> partitionMock;
  @Mock
  private Partitioner.PartitioningContext partitionContextMock;
  @Mock
  private Iterator<Partition<BandwidthLimitingOperator>> iteratorMock;
  private BandwidthPartitioner<BandwidthLimitingOperator> underTest = new BandwidthPartitioner<BandwidthLimitingOperator>();

  @Before
  public void setup()
  {
    MockitoAnnotations.initMocks(this);
    when(iteratorMock.hasNext()).thenReturn(true, false);
    when(iteratorMock.next()).thenReturn(partitionMock);
    when(partitionMock.getPartitionedInstance()).thenReturn(operatorMock);
    when(operatorMock.getBandwidthManager()).thenReturn(bandwidthManagerMock);
    when(bandwidthManagerMock.getBandwidth()).thenReturn(10L);
    when(partitionContextMock.getInputPorts()).thenReturn(null);
  }

  @Test
  public void testBandwidthOnPartitions()
  {
    when(partitionContextMock.getParallelPartitionCount()).thenReturn(0); // no partitions
    Collection<Partition<BandwidthLimitingOperator>> partitions = Lists.newArrayList();
    DefaultPartition<BandwidthLimitingOperator> defaultPartition = new DefaultPartition<BandwidthLimitingOperator>(operatorMock);
    partitions.add(defaultPartition);

    underTest.definePartitions(partitions, partitionContextMock);
    verify(bandwidthManagerMock).setBandwidth(10L);
  }

  @Test
  public void testBandwidthOnIncresedPartitions()
  {
    when(partitionContextMock.getParallelPartitionCount()).thenReturn(5);
    Collection<Partition<BandwidthLimitingOperator>> partitions = Lists.newArrayList();
    DefaultPartition<BandwidthLimitingOperator> defaultPartition = new DefaultPartition<BandwidthLimitingOperator>(operatorMock);
    partitions.add(defaultPartition);

    underTest.definePartitions(partitions, partitionContextMock);
    verify(bandwidthManagerMock, times(5)).setBandwidth(2L);
  }

  @Test
  public void testBandwidthOnReducedPartitions()
  {
    when(partitionContextMock.getParallelPartitionCount()).thenReturn(2);
    when(bandwidthManagerMock.getBandwidth()).thenReturn(2L);
    Collection<Partition<BandwidthLimitingOperator>> partitions = Lists.newArrayList();

    for (int i = 5; i-- > 0;) {
      partitions.add(new DefaultPartition<BandwidthLimitingOperator>(operatorMock));
    }

    underTest.definePartitions(partitions, partitionContextMock);
    verify(bandwidthManagerMock, times(2)).setBandwidth(5L);
  }

}

