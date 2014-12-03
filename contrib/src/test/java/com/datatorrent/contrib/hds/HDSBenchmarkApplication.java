/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
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
package com.datatorrent.contrib.hds;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.TreeMap;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Rule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.Context.PortContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.LocalMode;
import com.datatorrent.api.Operator;
import com.datatorrent.api.Stats.OperatorStats;
import com.datatorrent.api.StatsListener;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.common.util.Slice;
import com.datatorrent.contrib.hds.hfile.HFileImpl;
import com.datatorrent.lib.util.KeyValPair;
import com.datatorrent.lib.util.TestUtils;
import com.google.common.collect.Lists;
import com.datatorrent.api.Context;

@ApplicationAnnotation(name="HDSBenchmarkApplication")
public class HDSBenchmarkApplication implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    TestStatsListener sl = new TestStatsListener();
    sl.adjustRate = conf.getBoolean("dt.hdsbench.adjustRate", false);
    TestGenerator gen = dag.addOperator("Generator", new TestGenerator());
    dag.setAttribute(gen, OperatorContext.STATS_LISTENERS, Lists.newArrayList((StatsListener)sl));
    TestStoreOperator store = dag.addOperator("Store", new TestStoreOperator());
    dag.setAttribute(store, OperatorContext.STATS_LISTENERS, Lists.newArrayList((StatsListener)sl));
    HDSFileAccessFSImpl hfa = new HFileImpl();
    hfa.setBasePath(this.getClass().getSimpleName());
    store.setFileStore(hfa);
    dag.setInputPortAttribute(store.input, PortContext.PARTITION_PARALLEL, true);
    dag.getOperatorMeta("Store").getAttributes().put(Context.OperatorContext.COUNTERS_AGGREGATOR, new HDSWriter.BucketIOStatAggregator());
    dag.addStream("Events", gen.data, store.input).setLocality(Locality.THREAD_LOCAL);
  }

  public static class TestGenerator extends BaseOperator implements InputOperator
  {
    public final transient DefaultOutputPort<KeyValPair<byte[], byte[]>> data = new DefaultOutputPort<KeyValPair<byte[], byte[]>>();
    int emitBatchSize = 1000;
    byte[] val = ByteBuffer.allocate(1000).putLong(1234).array();
    int rate = 20000;
    int emitCount = 0;
    private final Random random = new Random();
    private int range = 1000*60; // one minute range of hot keys

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
      for (int i=0; i<emitBatchSize && emitCount < rate; i++) {
        byte[] key = ByteBuffer.allocate(16).putLong((timestamp - timestamp % range) + random.nextInt(range)).putLong(i).array();
        data.emit(new KeyValPair<byte[], byte[]>(key, val));
        emitCount++;
      }
    }
  }

  public static class TestStoreOperator extends HDSTestOperator
  {
    @Override
    protected void processEvent(KeyValPair<byte[], byte[]> event) throws IOException
    {
      super.processEvent(event);
    }
  }

  public static class TestStatsListener implements StatsListener, Serializable
  {
    private static final Logger LOG = LoggerFactory.getLogger(TestStatsListener.class);
    private static final long serialVersionUID = 1L;
    SetPropertyCommand cmd = new SetPropertyCommand();

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
        OperatorStats os = stats.getLastWindowedStats().get(stats.getLastWindowedStats().size()-1);
        if (os.inputPorts != null && !os.inputPorts.isEmpty()) {
          dwId = os.windowId;
          queueSize = os.inputPorts.get(0).queueSize;
          if (uwId - dwId < 5) {
            // keep operator busy
            rate = Math.max(1000, rate);
            rate += rate/10;
          } else if (uwId - dwId > 20){
            // operator is behind
            if (resumewid < dwId) {
              resumewid = uwId-15;
              rate -= rate/10;
            }
          }
        } else {
          LOG.debug("uwid-dwid {} skip {} rate {}, queueSize {}", uwId-dwId, resumewid-dwId, rate, queueSize);
          // upstream operator
          uwId = os.windowId;
          if (adjustRate) {
            Response rsp = new Response();
            cmd.rate = resumewid < dwId ? rate : 0;
            rsp.operatorCommands = Lists.newArrayList(cmd);
            return rsp;
          }
        }
      }
      return null;
    }

    public static class SetPropertyCommand implements OperatorCommand, Serializable
    {
      private static final long serialVersionUID = 1L;
      int rate;
      @Override
      public void execute(Operator oper, int arg1, long arg2) throws IOException
      {
        if (oper instanceof TestGenerator) {
          LOG.debug("Setting rate to {}", rate);
          ((TestGenerator)oper).rate = rate;
        }
      }
    }
  }

  @Rule
  public final TestUtils.TestInfo testInfo = new TestUtils.TestInfo();

  //@Test
  public void test() throws Exception
  {
    File file = new File(testInfo.getDir());
    FileUtils.deleteDirectory(file);

    LocalMode lma = LocalMode.newInstance();
    Configuration conf = new Configuration(false);
    conf.set("dt.operator.Store.fileStore.basePath", file.toURI().toString());
    //conf.set("dt.operator.Store.flushSize", "0");
    conf.set("dt.operator.Store.flushIntervalCount", "1");
    conf.set("dt.operator.Generator.attr.PARTITIONER", "com.datatorrent.lib.partitioner.StatelessPartitioner:2");

    lma.prepareDAG(new HDSTestApp(), conf);
    LocalMode.Controller lc = lma.getController();
    lc.setHeartbeatMonitoringEnabled(false);
    lc.runAsync();

    long tms = System.currentTimeMillis();
    File f0 = new File(file, "0/0-0");
    File f1 = new File(file, "1/1-0");
    File wal0 = new File(file, "0/_WAL-0");
    File wal1 = new File(file, "1/_WAL-0");

    while (System.currentTimeMillis() - tms < 30000) {
      if (f0.exists() && f1.exists()) break;
      Thread.sleep(100);
    }
    lc.shutdown();

    Assert.assertTrue("exists " + f0, f0.exists() && f0.isFile());
    Assert.assertTrue("exists " + f1, f1.exists() && f1.isFile());
    Assert.assertTrue("exists " + wal0, wal0.exists() && wal0.exists());
    Assert.assertTrue("exists " + wal1, wal1.exists() && wal1.exists());

    HDSFileAccessFSImpl fs = new MockFileAccess();
    fs.setBasePath(file.toURI().toString());
    fs.init();

    TreeMap<Slice, byte[]> data = new TreeMap<Slice, byte[]>(new HDSTest.SequenceComparator());
    fs.getReader(0, "0-0").readFully(data);
    Assert.assertFalse(data.isEmpty());

    data.clear();
    fs.getReader(1, "1-0").readFully(data);
    Assert.assertFalse(data.isEmpty());

    fs.close();
  }

}
