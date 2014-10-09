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

import com.datatorrent.lib.testbench.CollectorTestSink;
import com.datatorrent.lib.util.KeyValPair;

import org.junit.Assert;
import org.junit.Test;

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
    KeyValPair<String, Double> pair = (KeyValPair<String, Double>) sink.collectedTuples.get(0);
    Assert.assertEquals("1st b sma", 52.5, pair.getValue(), 0);
    pair = (KeyValPair<String, Double>) sink.collectedTuples.get(1);
    Assert.assertEquals("1st a sma", 31.5, pair.getValue(), 0);

    oper.beginWindow(1);
    oper.data.process(new KeyValPair<String, Double>("a", ++val));
    oper.data.process(new KeyValPair<String, Double>("a", ++val));
    oper.data.process(new KeyValPair<String, Double>("b", ++val2));
    oper.data.process(new KeyValPair<String, Double>("b", ++val2));
    oper.endWindow();
    Assert.assertEquals("number emitted tuples", 4, sink.collectedTuples.size());
    pair = (KeyValPair<String, Double>) sink.collectedTuples.get(2);
    Assert.assertEquals("2nd b sma", 53.5, pair.getValue(), 0);
    pair = (KeyValPair<String, Double>) sink.collectedTuples.get(3);
    Assert.assertEquals("2nd a sma", 32.5, pair.getValue(), 0);

    oper.beginWindow(2);
    oper.data.process(new KeyValPair<String, Double>("a", ++val));
    oper.data.process(new KeyValPair<String, Double>("a", ++val));
    oper.data.process(new KeyValPair<String, Double>("b", ++val2));
    oper.data.process(new KeyValPair<String, Double>("b", ++val2));
    oper.endWindow();
    Assert.assertEquals("number emitted tuples", 6, sink.collectedTuples.size());
    pair = (KeyValPair<String, Double>) sink.collectedTuples.get(4);
    Assert.assertEquals("2nd b sma", 54.5, pair.getValue(), 0);
    pair = (KeyValPair<String, Double>) sink.collectedTuples.get(5);
    Assert.assertEquals("2nd a sma", 33.5, pair.getValue(), 0);

    oper.beginWindow(3);
    oper.data.process(new KeyValPair<String, Double>("a", ++val));
    oper.data.process(new KeyValPair<String, Double>("a", ++val));
    oper.data.process(new KeyValPair<String, Double>("b", ++val2));
    oper.data.process(new KeyValPair<String, Double>("b", ++val2));
    oper.endWindow();
    Assert.assertEquals("number emitted tuples", 8, sink.collectedTuples.size());
    pair = (KeyValPair<String, Double>) sink.collectedTuples.get(4);
    Assert.assertEquals("2nd b sma", 54.5, pair.getValue(), 0);
    pair = (KeyValPair<String, Double>) sink.collectedTuples.get(5);
    Assert.assertEquals("2nd a sma", 33.5, pair.getValue(), 0);
  }
}
