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
package com.datatorrent.lib.async;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.lib.testbench.CollectorTestSink;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class AbstractAsyncProcessorTest
{
  public static class TestAsyncProcessor extends AbstractAsyncProcessor<String, String>
  {
    public transient final DefaultInputPort<String> input = new DefaultInputPort<String>()
    {
      @Override public void process(String s)
      {
        enqueueTupleForProcessing(s);
      }
    };
    public transient final DefaultOutputPort<String> output = new DefaultOutputPort<>();
    public transient final DefaultOutputPort<String> error = new DefaultOutputPort<>();

    @Override protected void handleProcessedTuple(String inpTuple, String resultTuple, State processState)
    {
      if (processState == State.SUCCESS) {
        output.emit(resultTuple);
      } else {
        error.emit(inpTuple);
      }
    }

    @Override protected String processTupleAsync(String tuple)
    {
      int i = Integer.parseInt(tuple) % 3;
      switch (i % 3) {
        case 1:
          return tuple + ";FIRSTPASS";
        case 2:
          return tuple + ";SECONDPASS";
        case 0:
        default:
          throw new RuntimeException("Expected");
      }
    }
  }

  @Test public void maintainOrderTest() throws InterruptedException
  {
    testOperator(1, true, 5000);
    testOperator(1, true, 15000);
  }

  @Test public void noOrderMaintainedTest() throws InterruptedException
  {
    testOperator(1, false, 5000);
    testOperator(2, false, 15000);
  }

  private void testOperator(int numThreads, boolean maintainOrder, long waitInterval) throws InterruptedException
  {
    CollectorTestSink<Object> sinkOut = new CollectorTestSink<>();
    CollectorTestSink<Object> sinkErr = new CollectorTestSink<>();

    TestAsyncProcessor async = new TestAsyncProcessor();
    async.setNumProcessors(numThreads);
    async.setMaintainTupleOrder(maintainOrder);
    async.output.setSink(sinkOut);
    async.error.setSink(sinkErr);
    async.setup(null);

    for (int windowId = 0; windowId < 10; windowId++) {
      async.beginWindow(windowId);
      for (int i = 0; i < 100; i++) {
        async.input.put(Integer.toString(windowId * 100 + i));
      }
      async.endWindow();
    }

    for (int i = 0; i < 10; i++) {
      Thread.sleep(waitInterval / 10);
      async.endWindow();
    }
    async.teardown();

    List<Object> outTuples = sinkOut.collectedTuples;
    List<Object> errTuples = sinkErr.collectedTuples;
    Assert.assertEquals(666, outTuples.size());
    Assert.assertEquals(334, errTuples.size());

    int previous = -1;
    for (Object outTuple : outTuples) {
      Assert.assertTrue(outTuple instanceof String);
      String outTuple1 = (String)outTuple;
      String[] split = outTuple1.split(";");
      Assert.assertEquals(2, split.length);
      int current = Integer.parseInt(split[0]);
      if (current % 3 == 1) {
        Assert.assertEquals("FIRSTPASS", split[1]);
      } else if (current % 3 == 2) {
        Assert.assertEquals("SECONDPASS", split[1]);
      } else if (current % 3 == 0) {
        Assert.assertTrue(false);
      }
      if (maintainOrder) {
        Assert.assertTrue(previous < current);
        previous = current;
      }
    }

    previous = -1;
    for (Object errTuple : errTuples) {
      Assert.assertTrue(errTuple instanceof String);
      String errTuple1 = (String)errTuple;
      String[] split = errTuple1.split(";");
      Assert.assertEquals(1, split.length);
      int current = Integer.parseInt(split[0]);
      Assert.assertTrue(current % 3 == 0);
      if (maintainOrder) {
        Assert.assertTrue(previous < current);
        previous = current;
      }
    }

  }
}
