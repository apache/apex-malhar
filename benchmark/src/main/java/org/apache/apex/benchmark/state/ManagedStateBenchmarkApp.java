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
package org.apache.apex.benchmark.state;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Random;

import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.fileaccess.TFileImpl;
import org.apache.apex.malhar.lib.state.managed.ManagedTimeUnifiedStateImpl;
import org.apache.apex.malhar.lib.util.KeyValPair;
import org.apache.hadoop.conf.Configuration;

import com.google.common.collect.Lists;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.Operator;
import com.datatorrent.api.Stats.OperatorStats;
import com.datatorrent.api.StatsListener;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.common.util.BaseOperator;

@ApplicationAnnotation(name = "ManagedStateBenchmark")
/**
 * @since 3.6.0
 */
public class ManagedStateBenchmarkApp implements StreamingApplication
{
  protected static final String PROP_STORE_PATH = "dt.application.ManagedStateBenchmark.storeBasePath";
  protected static final String DEFAULT_BASE_PATH = "ManagedStateBenchmark/Store";

  protected StoreOperator storeOperator;
  protected int timeRange = 1000 * 60; // one minute range of hot keys

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    TestStatsListener sl = new TestStatsListener();
    sl.adjustRate = conf.getBoolean("dt.ManagedStateBenchmark.adjustRate", false);
    TestGenerator gen = dag.addOperator("Generator", new TestGenerator());
    gen.setRange(timeRange);
    dag.setAttribute(gen, OperatorContext.STATS_LISTENERS, Lists.newArrayList((StatsListener)sl));

    storeOperator = new StoreOperator();
    storeOperator.setStore(createStore(conf));
    storeOperator.setTimeRange(timeRange);
    storeOperator = dag.addOperator("Store", storeOperator);

    dag.setAttribute(storeOperator, OperatorContext.STATS_LISTENERS, Lists.newArrayList((StatsListener)sl));

    dag.addStream("Events", gen.data, storeOperator.input).setLocality(Locality.CONTAINER_LOCAL);
  }

  public ManagedTimeUnifiedStateImpl createStore(Configuration conf)
  {
    String basePath = getStoreBasePath(conf);
    ManagedTimeUnifiedStateImpl store = new ManagedTimeUnifiedStateImpl();
    ((TFileImpl.DTFileImpl)store.getFileAccess()).setBasePath(basePath);
    store.getTimeBucketAssigner().setBucketSpan(Duration.millis(10000));
    return store;
  }

  public String getStoreBasePath(Configuration conf)
  {

    String basePath = conf.get(PROP_STORE_PATH);
    if (basePath == null || basePath.isEmpty()) {
      basePath = DEFAULT_BASE_PATH;
    }
    return basePath;
  }

  public static class TestGenerator extends BaseOperator implements InputOperator
  {
    public final transient DefaultOutputPort<KeyValPair<byte[], byte[]>> data =
        new DefaultOutputPort<KeyValPair<byte[], byte[]>>();
    int emitBatchSize = 1000;
    byte[] val = ByteBuffer.allocate(1000).putLong(1234).array();
    int rate = 20000;
    int emitCount = 0;
    private final Random random = new Random();
    private int range = 1000 * 60; // one minute range of hot keys

    public int getEmitBatchSize()
    {
      return emitBatchSize;
    }

    public void setEmitBatchSize(int emitBatchSize)
    {
      this.emitBatchSize = emitBatchSize;
    }

    public int getRate()
    {
      return rate;
    }

    public void setRate(int rate)
    {
      this.rate = rate;
    }

    public int getRange()
    {
      return range;
    }

    public void setRange(int range)
    {
      this.range = range;
    }

    @Override
    public void beginWindow(long windowId)
    {
      super.beginWindow(windowId);
      emitCount = 0;
    }

    @Override
    public void emitTuples()
    {
      long timestamp = System.currentTimeMillis();
      for (int i = 0; i < emitBatchSize && emitCount < rate; i++) {
        byte[] key = ByteBuffer.allocate(16).putLong((timestamp - timestamp % range) + random.nextInt(range)).putLong(i)
            .array();
        data.emit(new KeyValPair<byte[], byte[]>(key, val));
        emitCount++;
      }
    }
  }

  public static class TestStatsListener implements StatsListener, Serializable
  {
    private static final Logger LOG = LoggerFactory.getLogger(TestStatsListener.class);
    private static final long serialVersionUID = 1L;
    SetPropertyRequest cmd = new SetPropertyRequest();

    long uwId;
    long dwId;
    long resumewid;
    int rate;
    int queueSize;
    boolean adjustRate;

    @Override
    public Response processStats(BatchedOperatorStats stats)
    {
      if (!stats.getLastWindowedStats().isEmpty()) {
        OperatorStats os = stats.getLastWindowedStats().get(stats.getLastWindowedStats().size() - 1);
        if (os.inputPorts != null && !os.inputPorts.isEmpty()) {
          dwId = os.windowId;
          queueSize = os.inputPorts.get(0).queueSize;
          if (uwId - dwId < 5) {
            // keep operator busy
            rate = Math.max(1000, rate);
            rate += rate / 10;
          } else if (uwId - dwId > 20) {
            // operator is behind
            if (resumewid < dwId) {
              resumewid = uwId - 15;
              rate -= rate / 10;
            }
          }
        } else {
          LOG.debug("uwid-dwid {} skip {} rate {}, queueSize {}", uwId - dwId, resumewid - dwId, rate, queueSize);
          // upstream operator
          uwId = os.windowId;
          if (adjustRate) {
            Response rsp = new Response();
            cmd.rate = resumewid < dwId ? rate : 0;
            rsp.operatorRequests = Lists.newArrayList(cmd);
            return rsp;
          }
        }
      }
      return null;
    }

    public static class SetPropertyRequest implements OperatorRequest, Serializable
    {
      private static final long serialVersionUID = 1L;
      int rate;

      @Override
      public OperatorResponse execute(Operator oper, int arg1, long arg2) throws IOException
      {
        if (oper instanceof TestGenerator) {
          LOG.debug("Setting rate to {}", rate);
          ((TestGenerator)oper).rate = rate;
        }
        return null;
      }
    }
  }

}
