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

package org.apache.apex.malhar.lib.io.fs;

import java.io.IOException;
import java.util.Collections;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.apache.apex.malhar.lib.counters.BasicCounters;
import org.apache.apex.malhar.lib.io.fs.AbstractFileInputOperator.FailedFile;
import org.apache.apex.malhar.lib.io.fs.AbstractFileInputOperator.FileCounters;
import org.apache.commons.lang.mutable.MutableLong;

import com.datatorrent.api.Partitioner.Partition;
import com.datatorrent.api.Stats.OperatorStats;
import com.datatorrent.api.StatsListener.BatchedOperatorStats;
import com.datatorrent.api.StatsListener.Response;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

public class AbstractThroughputFileInputOperatorTest
{
  private AbstractThroughputFileInputOperator<String> underTest;
  @Mock
  private Partition<AbstractFileInputOperator<String>> mockPartition;
  @Mock
  private BatchedOperatorStats mockBatchStats;
  @Mock
  private OperatorStats mockOperatorStats;
  @Mock
  private BasicCounters<MutableLong> fileCountersMock;
  @Mock
  private MutableLong fileCounterMock;

  @Before
  public void setup()
  {
    underTest = new ThroughputFileInputOperator();
    MockitoAnnotations.initMocks(this);

    when(mockPartition.getPartitionedInstance()).thenReturn(underTest);
  }

  @Test
  public void testInitialPartitioning()
  {
    underTest.setPartitionCount(4);
    underTest.setPreferredMaxPendingFilesPerOperator(6);

    for (int i = 0; i < 74; i++) {
      underTest.pendingFiles.add("file-" + i);
    }

    int partitioncount = underTest.getNewPartitionCount(Collections.singleton(mockPartition), null);
    Assert.assertEquals(4, partitioncount);
  }

  @Test
  public void testProcessStats() throws Exception
  {
    underTest.setPartitionCount(4);
    underTest.setPreferredMaxPendingFilesPerOperator(10);

    for (int i = 0; i < 21; i++) {
      underTest.pendingFiles.add("file-" + i);
    }

    mockOperatorStats.counters = fileCountersMock;
    when(mockPartition.getStats()).thenReturn(mockBatchStats);
    when(mockBatchStats.getLastWindowedStats()).thenReturn(Collections.singletonList(mockOperatorStats));
    when(fileCountersMock.getCounter(any(FileCounters.class))).thenReturn(fileCounterMock);
    when(fileCounterMock.getValue()).thenReturn(20L);

    Response response = underTest.processStats(mockBatchStats);

    Assert.assertTrue(response.repartitionRequired);
  }

  @Test
  public void testRepartitionNotRequired()
  {
    underTest.setPartitionCount(4);
    underTest.setPreferredMaxPendingFilesPerOperator(10);
    underTest.setRepartitionInterval(60 * 1000);
    underTest.lastRepartition = System.currentTimeMillis();

    for (int i = 0; i < 10; i++) {
      underTest.pendingFiles.add("file-" + i);
    }

    mockOperatorStats.counters = fileCountersMock;
    when(mockPartition.getStats()).thenReturn(mockBatchStats);
    when(mockBatchStats.getLastWindowedStats()).thenReturn(Collections.singletonList(mockOperatorStats));
    when(fileCountersMock.getCounter(any(FileCounters.class))).thenReturn(fileCounterMock);
    when(fileCounterMock.getValue()).thenReturn(10L);

    Response response = underTest.processStats(mockBatchStats);

    Assert.assertFalse(response.repartitionRequired);
  }

  @Test
  public void testRepartitioningForPendingFiles()
  {
    underTest.setPartitionCount(4);
    underTest.setPreferredMaxPendingFilesPerOperator(10);

    for (int i = 0; i < 21; i++) {
      underTest.pendingFiles.add("file-" + i);
    }

    when(mockPartition.getStats()).thenReturn(mockBatchStats);
    int partitioncount = underTest.getNewPartitionCount(Collections.singleton(mockPartition), null);
    Assert.assertEquals(3, partitioncount);
  }

  @Test
  public void testRepartitioningForFailedFiles()
  {
    underTest.setPartitionCount(6);
    underTest.setPreferredMaxPendingFilesPerOperator(6);

    for (int i = 0; i < 21; i++) {
      underTest.failedFiles.add(new FailedFile("file-" + i, 0));
    }

    when(mockPartition.getStats()).thenReturn(mockBatchStats);
    int partitioncount = underTest.getNewPartitionCount(Collections.singleton(mockPartition), null);
    Assert.assertEquals(4, partitioncount);
  }

  @Test
  public void testRepartitioningForUnfinishedFiles()
  {
    underTest.setPartitionCount(5);
    underTest.setPreferredMaxPendingFilesPerOperator(3);

    for (int i = 0; i < 21; i++) {
      underTest.pendingFiles.add("file-" + i);
    }

    when(mockPartition.getStats()).thenReturn(mockBatchStats);
    int partitioncount = underTest.getNewPartitionCount(Collections.singleton(mockPartition), null);
    Assert.assertEquals(5, partitioncount);
  }

  public static class ThroughputFileInputOperator extends AbstractThroughputFileInputOperator<String>
  {
    @Override
    protected String readEntity() throws IOException
    {
      return "testData";
    }

    @Override
    protected void emit(String tuple)
    {
    }
  }
}
