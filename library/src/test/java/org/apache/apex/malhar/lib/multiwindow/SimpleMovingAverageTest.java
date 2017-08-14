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
package org.apache.apex.malhar.lib.multiwindow;

import org.junit.Assert;
import org.junit.Test;

import org.apache.apex.malhar.lib.testbench.CollectorTestSink;
import org.apache.apex.malhar.lib.util.KeyValPair;

/**
 * Functional test for {@link org.apache.apex.malhar.lib.multiwindow.SimpleMovingAverage}. <p>
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
    for (int i = 0; i < 2; i++) {
      KeyValPair<String, Double> pair = (KeyValPair<String, Double>)sink.collectedTuples.get(i);
      if (pair.getKey().equals("a")) {
        Assert.assertEquals("a SMA", 31.5, pair.getValue(), 0);
      } else {
        Assert.assertEquals("b SMA", 52.5, pair.getValue(), 0);
      }
    }

    oper.beginWindow(1);
    oper.data.process(new KeyValPair<String, Double>("a", ++val));
    oper.data.process(new KeyValPair<String, Double>("a", ++val));
    oper.data.process(new KeyValPair<String, Double>("b", ++val2));
    oper.data.process(new KeyValPair<String, Double>("b", ++val2));
    oper.endWindow();
    Assert.assertEquals("number emitted tuples", 4, sink.collectedTuples.size());
    for (int i = 2; i < 4; i++) {
      KeyValPair<String, Double> pair = (KeyValPair<String, Double>)sink.collectedTuples.get(i);
      if (pair.getKey().equals("a")) {
        Assert.assertEquals("a SMA", 32.5, pair.getValue(), 0);
      } else {
        Assert.assertEquals("b SMA", 53.5, pair.getValue(), 0);
      }
    }

    oper.beginWindow(2);
    oper.data.process(new KeyValPair<String, Double>("a", ++val));
    oper.data.process(new KeyValPair<String, Double>("a", ++val));
    oper.data.process(new KeyValPair<String, Double>("b", ++val2));
    oper.data.process(new KeyValPair<String, Double>("b", ++val2));
    oper.endWindow();
    Assert.assertEquals("number emitted tuples", 6, sink.collectedTuples.size());
    for (int i = 4; i < 6; i++) {
      KeyValPair<String, Double> pair = (KeyValPair<String, Double>)sink.collectedTuples.get(i);
      if (pair.getKey().equals("a")) {
        Assert.assertEquals("a SMA", 33.5, pair.getValue(), 0);
      } else {
        Assert.assertEquals("b SMA", 54.5, pair.getValue(), 0);
      }
    }

    oper.beginWindow(3);
    oper.data.process(new KeyValPair<String, Double>("a", ++val));
    oper.data.process(new KeyValPair<String, Double>("a", ++val));
    oper.data.process(new KeyValPair<String, Double>("b", ++val2));
    oper.data.process(new KeyValPair<String, Double>("b", ++val2));
    oper.endWindow();
    Assert.assertEquals("number emitted tuples", 8, sink.collectedTuples.size());
    for (int i = 6; i < 8; i++) {
      KeyValPair<String, Double> pair = (KeyValPair<String, Double>)sink.collectedTuples.get(i);
      if (pair.getKey().equals("a")) {
        Assert.assertEquals("a SMA", 35.5, pair.getValue(), 0);
      } else {
        Assert.assertEquals("b SMA", 56.5, pair.getValue(), 0);
      }
    }
  }
}
