/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
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
package com.datatorrent.lib.io.fs;

import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Lists;

import com.datatorrent.api.DefaultOutputPort;

import com.datatorrent.lib.testbench.CollectorTestSink;
import com.datatorrent.lib.testbench.RandomWordGenerator;
import com.datatorrent.stram.StramLocalCluster;
import com.datatorrent.stram.plan.logical.LogicalPlan;

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

  @Test
  public void testReconciler()
  {
    try {
      LogicalPlan dag = new LogicalPlan();

      RandomWordGenerator generator = dag.addOperator("words", new RandomWordGenerator());
      TestReconciler reconciler = dag.addOperator("synchronizer", new TestReconciler());

      generator.setTuplesPerWindow(10);

      dag.addStream("toWriter", generator.output, reconciler.input);

      StramLocalCluster slc = new StramLocalCluster(dag);

      slc.run(150000); // test assert in synchronizer.processCommittedData to ensure it is called only for TestMeta with committed window ids
    }
    catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  public static class TestReconciler1 extends AbstractReconciler<String, String>
  {

     public transient DefaultOutputPort<String> outputPort = new DefaultOutputPort<String>();
    @Override
    protected void processTuple(String input)
    {
      enqueueForProcessing(input);
    }

    @Override
    protected void processCommittedData(String meta)
    {
      outputPort.emit(meta);
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
    reconciler1.beginWindow(windowId++);
    reconciler1.input.process("e");
    reconciler1.input.process("f");
    reconciler1.endWindow();
    reconciler1.beginWindow(windowId++);
    reconciler1.input.process("g");
    reconciler1.input.process("h");
    reconciler1.endWindow();
    Thread.sleep(500);
    output.add("a");
    output.add("b");
    Assert.assertEquals(output,sink.collectedTuples);
    output.clear();
    sink.collectedTuples.clear();
    reconciler1.committed(2);
    Thread.sleep(500);
    output.add("c");
    output.add("d");
    output.add("e");
    output.add("f");
    Assert.assertEquals(output,sink.collectedTuples);
    reconciler1.teardown();
  }

}