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
package org.apache.apex.benchmark.window;

import java.io.IOException;
import java.io.Serializable;

import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.benchmark.window.WindowedOperatorBenchmarkApp.WindowedGenerator;
import org.apache.apex.malhar.lib.fileaccess.TFileImpl;
import org.apache.apex.malhar.lib.state.managed.UnboundedTimeBucketAssigner;
import org.apache.apex.malhar.lib.state.spillable.SpillableComplexComponentImpl;
import org.apache.apex.malhar.lib.state.spillable.SpillableStateStore;
import org.apache.apex.malhar.lib.state.spillable.managed.ManagedTimeUnifiedStateSpillableStateStore;
import org.apache.apex.malhar.lib.stream.DevNull;
import org.apache.apex.malhar.lib.window.Accumulation;
import org.apache.apex.malhar.lib.window.TriggerOption;
import org.apache.apex.malhar.lib.window.WindowOption;
import org.apache.apex.malhar.lib.window.WindowedStorage;
import org.apache.apex.malhar.lib.window.impl.AbstractWindowedOperator;
import org.apache.apex.malhar.lib.window.impl.InMemoryWindowedStorage;
import org.apache.hadoop.conf.Configuration;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.Operator;
import com.datatorrent.api.Stats.OperatorStats;
import com.datatorrent.api.StatsListener;
import com.datatorrent.api.StreamingApplication;

/**
 * @since 3.7.0
 */
public abstract class AbstractWindowedOperatorBenchmarkApp<G extends Operator, O extends AbstractWindowedOperator>
    implements StreamingApplication
{
  protected static final String PROP_STORE_PATH = "dt.application.WindowedOperatorBenchmark.storeBasePath";
  protected static final String DEFAULT_BASE_PATH = "WindowedOperatorBenchmark/Store";
  protected static final int ALLOWED_LATENESS = 19000;

  protected int timeRange = 1000 * 60; // one minute range of hot keys

  protected Class<G> generatorClass;
  protected Class<O> windowedOperatorClass;

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    TestStatsListener sl = new TestStatsListener();
    sl.adjustRate = conf.getBoolean("dt.ManagedStateBenchmark.adjustRate", false);

    G generator = createGenerator();
    dag.addOperator("Generator", generator);
    //generator.setRange(timeRange);
    dag.setAttribute(generator, OperatorContext.STATS_LISTENERS, Lists.newArrayList((StatsListener)sl));

    O windowedOperator = createWindowedOperator(conf);
    dag.addOperator("windowedOperator", windowedOperator);
    dag.setAttribute(windowedOperator, OperatorContext.STATS_LISTENERS, Lists.newArrayList((StatsListener)sl));
    //dag.addStream("Data", generator.data, windowedOperator.input).setLocality(Locality.CONTAINER_LOCAL);
    connectGeneratorToWindowedOperator(dag, generator, windowedOperator);

//    WatermarkGenerator watermarkGenerator = new WatermarkGenerator();
//    dag.addOperator("WatermarkGenerator", watermarkGenerator);
//    dag.addStream("Control", watermarkGenerator.control, windowedOperator.controlInput)
//      .setLocality(Locality.CONTAINER_LOCAL);

    DevNull output = dag.addOperator("output", new DevNull());
    dag.addStream("output", windowedOperator.output, output.data).setLocality(Locality.CONTAINER_LOCAL);
  }

  protected G createGenerator()
  {
    try {
      return generatorClass.newInstance();
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  protected O createWindowedOperator(Configuration conf)
  {
    SpillableStateStore store = createStore(conf);
    try {
      O windowedOperator = this.windowedOperatorClass.newInstance();
      SpillableComplexComponentImpl sccImpl = new SpillableComplexComponentImpl(store);
      windowedOperator.addComponent("SpillableComplexComponent", sccImpl);

      windowedOperator.setDataStorage(createDataStorage(sccImpl));
      windowedOperator.setRetractionStorage(createRetractionStorage(sccImpl));
      windowedOperator.setWindowStateStorage(new InMemoryWindowedStorage());
      setUpdatedKeyStorage(windowedOperator, conf, sccImpl);
      windowedOperator.setAccumulation(createAccumulation());

      windowedOperator.setAllowedLateness(Duration.millis(ALLOWED_LATENESS));
      windowedOperator.setWindowOption(new WindowOption.TimeWindows(Duration.standardMinutes(1)));
      //accumulating mode
      windowedOperator.setTriggerOption(TriggerOption.AtWatermark()
          .withEarlyFiringsAtEvery(Duration.standardSeconds(1)).accumulatingFiredPanes().firingOnlyUpdatedPanes());
      windowedOperator.setFixedWatermark(30000);
      //windowedOperator.setTriggerOption(TriggerOption.AtWatermark());

      return windowedOperator;
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  protected void setUpdatedKeyStorage(O windowedOperator, Configuration conf, SpillableComplexComponentImpl sccImpl)
  {
  }

  protected abstract WindowedStorage createDataStorage(SpillableComplexComponentImpl sccImpl);

  protected abstract WindowedStorage createRetractionStorage(SpillableComplexComponentImpl sccImpl);

  protected abstract Accumulation createAccumulation();

  protected abstract void connectGeneratorToWindowedOperator(DAG dag, G generator, O windowedOperator);

  protected SpillableStateStore createStore(Configuration conf)
  {
    String basePath = getStoreBasePath(conf);
    ManagedTimeUnifiedStateSpillableStateStore store = new ManagedTimeUnifiedStateSpillableStateStore();
    store.setTimeBucketAssigner(new UnboundedTimeBucketAssigner());
    store.getTimeBucketAssigner().setBucketSpan(Duration.millis(10000));
    ((TFileImpl.DTFileImpl)store.getFileAccess()).setBasePath(basePath);

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
          //LOG.debug("uwid-dwid {} skip {} rate {}, queueSize {}", uwId - dwId, resumewid - dwId, rate, queueSize);
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
        if (oper instanceof WindowedGenerator) {
          LOG.debug("Setting rate to {}", rate);
          ((WindowedGenerator)oper).rate = rate;
        }
        return null;
      }
    }
  }
}
