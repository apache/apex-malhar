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
package com.datatorrent.lib.multiwindow;

import java.util.List;

import com.datatorrent.lib.testbench.CollectorTestSink;
import com.datatorrent.lib.util.KeyValPair;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Lists;

/**
 * Functional test for {@link com.datatorrent.lib.multiwindow.SimpleMovingAverage}. <p>
 */
public class SimpleMovingAverageTest
{
  /**
   * Test functional logic
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  @Test
  public void testNodeProcessing() throws InterruptedException
  {
    SimpleMovingAverage<String, Double> oper = new SimpleMovingAverage<String, Double>();

    CollectorTestSink sink = new CollectorTestSink();
    CollectorTestSink sink2 = new CollectorTestSink();
    oper.doubleSMA.setSink(sink);
    oper.integerSMA.setSink(sink2);
    oper.setWindowSize(3);

    double val = 30;
    double val2 = 51;
    oper.beginWindow(0);
    oper.data.process(new KeyValPair<String, Double>("a", ++val));
    oper.data.process(new KeyValPair<String, Double>("a", ++val));
    oper.data.process(new KeyValPair<String, Double>("b", ++val2));
    oper.data.process(new KeyValPair<String, Double>("b", ++val2));
    oper.endWindow();
    Assert.assertEquals("number emitted tuples", 2, sink.collectedTuples.size());
    List<KeyValPair<String, Double>> outputList = Lists.newArrayList();
    outputList.add(new KeyValPair<String, Double>("b", 52.5));
    outputList.add(new KeyValPair<String, Double>("a", 31.5));
    Assert.assertEquals("SMA", outputList, sink.collectedTuples);

    oper.beginWindow(1);
    oper.data.process(new KeyValPair<String, Double>("a", ++val));
    oper.data.process(new KeyValPair<String, Double>("a", ++val));
    oper.data.process(new KeyValPair<String, Double>("b", ++val2));
    oper.data.process(new KeyValPair<String, Double>("b", ++val2));
    oper.endWindow();
    Assert.assertEquals("number emitted tuples", 4, sink.collectedTuples.size());
    outputList.add(new KeyValPair<String, Double>("b", 53.5));
    outputList.add(new KeyValPair<String, Double>("a", 32.5));
    Assert.assertEquals("SMA", outputList, sink.collectedTuples);

    oper.beginWindow(2);
    oper.data.process(new KeyValPair<String, Double>("a", ++val));
    oper.data.process(new KeyValPair<String, Double>("a", ++val));
    oper.data.process(new KeyValPair<String, Double>("b", ++val2));
    oper.data.process(new KeyValPair<String, Double>("b", ++val2));
    oper.endWindow();
    Assert.assertEquals("number emitted tuples", 6, sink.collectedTuples.size());
    outputList.add(new KeyValPair<String, Double>("b", 54.5));
    outputList.add(new KeyValPair<String, Double>("a", 33.5));
    Assert.assertEquals("SMA", outputList, sink.collectedTuples);

    oper.beginWindow(3);
    oper.data.process(new KeyValPair<String, Double>("a", ++val));
    oper.data.process(new KeyValPair<String, Double>("a", ++val));
    oper.data.process(new KeyValPair<String, Double>("b", ++val2));
    oper.data.process(new KeyValPair<String, Double>("b", ++val2));
    oper.endWindow();
    Assert.assertEquals("number emitted tuples", 8, sink.collectedTuples.size());
    outputList.add(new KeyValPair<String, Double>("b", 56.5));
    outputList.add(new KeyValPair<String, Double>("a", 35.5));
    Assert.assertEquals("SMA", outputList, sink.collectedTuples);
  }
}
