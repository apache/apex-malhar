/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;

import org.junit.rules.TestWatcher;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import com.datatorrent.api.*;
import com.datatorrent.api.Operator.OutputPort;

public class TestUtils
{
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

  /**
   * Clone object by serializing and deserializing using Kryo.
   * Note this is different from using {@link Kryo#copy(Object)}, which will attempt to also clone transient fields.
   * @param kryo
   * @param src
   * @return
   * @throws IOException
   */
  public static <T> T clone(Kryo kryo, T src) throws IOException
  {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    Output output = new Output(bos);
    kryo.writeObject(output, src);
    output.close();
    Input input = new Input(bos.toByteArray());
    @SuppressWarnings("unchecked")
    Class<T> clazz = (Class<T>)src.getClass();
    return kryo.readObject(input, clazz);
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
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
