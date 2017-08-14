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
package org.apache.apex.malhar.lib.util;

import java.io.File;
import java.util.List;

import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import org.apache.apex.malhar.lib.utils.serde.BufferSlice;
import org.apache.commons.io.FileUtils;

import com.google.common.base.Preconditions;

import com.datatorrent.api.DefaultPartition;
import com.datatorrent.api.Operator;
import com.datatorrent.api.Operator.OutputPort;
import com.datatorrent.api.Sink;
import com.datatorrent.api.Stats;
import com.datatorrent.api.StatsListener;
import com.datatorrent.netlet.util.Slice;

public class TestUtils
{
  public static byte[] getByte(int val)
  {
    Preconditions.checkArgument(val <= Byte.MAX_VALUE);
    return new byte[]{(byte)val};
  }

  public static byte[] getBytes(int val)
  {
    byte[] bytes = new byte[4];
    bytes[0] = (byte)(val & 0xFF);
    bytes[1] = (byte)((val >> 8) & 0xFF);
    bytes[2] = (byte)((val >> 16) & 0xFF);
    bytes[3] = (byte)((val >> 24) & 0xFF);

    return bytes;
  }

  public static Slice getSlice(int val)
  {
    return new BufferSlice(getBytes(val));
  }

  public static class TestInfo extends TestWatcher
  {
    public org.junit.runner.Description desc;

    public String getDir()
    {
      String methodName = desc.getMethodName();
      String className = desc.getClassName();
      return "target/" + className + "/" + methodName;
    }

    @Override
    protected void starting(org.junit.runner.Description description)
    {
      this.desc = description;
    }
  }

  public static void deleteTargetTestClassFolder(Description description)
  {
    FileUtils.deleteQuietly(new File("target/" + description.getClassName()));
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  public static <S extends Sink, T> S setSink(OutputPort<T> port, S sink)
  {
    port.setSink(sink);
    return sink;
  }

  /**
   * A mock batched operator stats used for testing stats listener and partitioner.
   */
  public static class MockBatchedOperatorStats implements StatsListener.BatchedOperatorStats
  {

    final int operatorId;
    public List<Stats.OperatorStats> operatorStats;
    public long latency;

    public MockBatchedOperatorStats(int operatorId)
    {
      this.operatorId = operatorId;
    }

    @Override
    public List<Stats.OperatorStats> getLastWindowedStats()
    {
      return operatorStats;
    }

    @Override
    public int getOperatorId()
    {
      return operatorId;
    }

    @Override
    public long getCurrentWindowId()
    {
      return 0;
    }

    @Override
    public long getTuplesProcessedPSMA()
    {
      return 0;
    }

    @Override
    public long getTuplesEmittedPSMA()
    {
      return 0;
    }

    @Override
    public double getCpuPercentageMA()
    {
      return 0;
    }

    @Override
    public long getLatencyMA()
    {
      return latency;
    }

    @Override
    public List<StatsListener.OperatorResponse> getOperatorResponse()
    {
      return null;
    }
  }

  /**
   * A mock {@link DefaultPartition}
   *
   * @param <T> operator type
   */
  public static class MockPartition<T extends Operator> extends DefaultPartition<T>
  {

    public MockPartition(DefaultPartition<T> defaultPartition, MockBatchedOperatorStats stats)
    {
      super(defaultPartition.getPartitionedInstance(), defaultPartition.getPartitionKeys(),
        defaultPartition.getLoad(), stats);
    }
  }

}
