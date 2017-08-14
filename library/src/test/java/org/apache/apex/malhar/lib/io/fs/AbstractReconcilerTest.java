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

import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import org.apache.apex.malhar.lib.testbench.CollectorTestSink;
import org.apache.apex.malhar.lib.testbench.RandomWordGenerator;
import org.apache.hadoop.conf.Configuration;

import com.google.common.collect.Lists;

import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.LocalMode;
import com.datatorrent.api.StreamingApplication;

/**
 * Test class to test {@link AbstractReconciler}
 */
public class AbstractReconcilerTest
{
  public static class TestReconciler extends AbstractReconciler<byte[], TestMeta>
  {
    private long currentCommittedWindow;
    private StringBuilder windowData = new StringBuilder();

    @Override
    protected void processTuple(byte[] input)
    {
      if (windowData.length() > 0) {
        windowData.append(",");
      }
      windowData.append(input);
    }

    @Override
    public void endWindow()
    {
      TestMeta meta = new TestMeta();
      meta.windowid = currentWindowId;
      meta.data = windowData.toString();
      enqueueForProcessing(meta);
      windowData.setLength(0);
    }

    @Override
    protected void processCommittedData(TestMeta meta)
    {
      // only committed windows have to be processed
      Assert.assertTrue("processing window id should not be greater than commited window id", meta.windowid <= currentCommittedWindow);
    }

    @Override
    public void committed(long l)
    {
      currentCommittedWindow = l;
      super.committed(l);
    }

  }

  public static class TestMeta
  {
    long windowid;
    String data;
  }

  public static class TestDag implements StreamingApplication
  {
    @Override
    public void populateDAG(DAG dag, Configuration conf)
    {
      RandomWordGenerator generator = dag.addOperator("words", new RandomWordGenerator());
      TestReconciler reconciler = dag.addOperator("synchronizer", new TestReconciler());
      generator.setTuplesPerWindow(10);
      dag.addStream("toWriter", generator.output, reconciler.input);
    }
  }

  @Test
  public void testReconciler() throws Exception
  {
    LocalMode lma = LocalMode.newInstance();
    Configuration configuration = new Configuration();
    lma.prepareDAG(new TestDag(), configuration);
    LocalMode.Controller lc = lma.getController();
    lc.setHeartbeatMonitoringEnabled(false);
    lc.run(15000);
    lc.shutdown();
  }

  public static class TestReconciler1 extends AbstractReconciler<String, String>
  {

    public transient DefaultOutputPort<String> outputPort = new DefaultOutputPort<String>();
    private List<String> emitData = new ArrayList<String>();

    @Override
    public void beginWindow(long windowId)
    {
      for (String data : emitData) {
        outputPort.emit(data);
      }
      emitData.clear();
      super.beginWindow(windowId);
    }

    @Override
    protected void processTuple(String input)
    {
      enqueueForProcessing(input);
    }

    @Override
    protected void processCommittedData(String meta)
    {
      emitData.add(meta);
    }
  }

  @Test
  public void testOperator() throws Exception
  {
    CollectorTestSink<Object> sink = new CollectorTestSink<Object>();
    TestReconciler1 reconciler1 = new TestReconciler1();
    List<String> output = Lists.newArrayList();
    reconciler1.outputPort.setSink(sink);
    reconciler1.setup(null);
    int windowId = 0;
    reconciler1.beginWindow(windowId++);
    reconciler1.input.process("a");
    reconciler1.input.process("b");
    reconciler1.endWindow();
    reconciler1.beginWindow(windowId++);
    reconciler1.input.process("c");
    reconciler1.input.process("d");
    reconciler1.endWindow();
    reconciler1.committed(0);
    Thread.sleep(500);
    reconciler1.beginWindow(windowId++);
    reconciler1.input.process("e");
    reconciler1.input.process("f");
    reconciler1.endWindow();
    reconciler1.beginWindow(windowId++);
    reconciler1.input.process("g");
    reconciler1.input.process("h");
    reconciler1.endWindow();
    output.add("a");
    output.add("b");
    Assert.assertEquals(output, sink.collectedTuples);
    output.clear();
    sink.collectedTuples.clear();
    reconciler1.committed(2);
    Thread.sleep(500);
    reconciler1.beginWindow(windowId++);
    reconciler1.endWindow();
    output.add("c");
    output.add("d");
    output.add("e");
    output.add("f");
    Assert.assertEquals(output, sink.collectedTuples);
    reconciler1.teardown();
  }

}
