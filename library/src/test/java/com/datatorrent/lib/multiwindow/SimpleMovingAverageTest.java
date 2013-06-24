/*
 * Copyright (c) 2013 Malhar Inc. ALL Rights Reserved.
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
 * limitations under the License. See accompanying LICENSE file.
 */
package com.datatorrent.lib.multiwindow;

import com.datatorrent.engine.TestSink;
import com.datatorrent.lib.multiwindow.SimpleMovingAverage;
import com.datatorrent.lib.util.KeyValPair;
import junit.framework.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Functional test for {@link com.datatorrent.lib.multiwindow.SimpleMovingAverage}. <p>
 *
 */
public class SimpleMovingAverageTest
{
  private static final Logger logger = LoggerFactory.getLogger(SimpleMovingAverageTest.class);

  /**
   * Test functional logic
   */
  @Test
  public void testNodeProcessing() throws InterruptedException
  {
    SimpleMovingAverage<String, Double> oper = new SimpleMovingAverage<String, Double>();

    TestSink sink = new TestSink();
    TestSink sink2 = new TestSink();
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

    oper.beginWindow(1);
    oper.data.process(new KeyValPair<String, Double>("a", ++val));
    oper.data.process(new KeyValPair<String, Double>("a", ++val));
    oper.data.process(new KeyValPair<String, Double>("b", ++val2));
    oper.data.process(new KeyValPair<String, Double>("b", ++val2));
    oper.endWindow();

    oper.beginWindow(2);
    oper.data.process(new KeyValPair<String, Double>("a", ++val));
    oper.data.process(new KeyValPair<String, Double>("a", ++val));
    oper.data.process(new KeyValPair<String, Double>("b", ++val2));
    oper.data.process(new KeyValPair<String, Double>("b", ++val2));
    oper.endWindow();

    oper.beginWindow(3);
    oper.data.process(new KeyValPair<String, Double>("a", ++val));
    oper.data.process(new KeyValPair<String, Double>("a", ++val));
    oper.data.process(new KeyValPair<String, Double>("b", ++val2));
    oper.data.process(new KeyValPair<String, Double>("b", ++val2));
    oper.endWindow();

    Assert.assertEquals("number emitted tuples", 8, sink.collectedTuples.size());
    Assert.assertEquals("1st sma", 52.5, ((KeyValPair<String, Double>)sink.collectedTuples.get(0)).getValue().doubleValue());
    Assert.assertEquals("2nd sma", 53.5, ((KeyValPair<String, Double>)sink.collectedTuples.get(2)).getValue().doubleValue());
    Assert.assertEquals("3rd sma", 54.5, ((KeyValPair<String, Double>)sink.collectedTuples.get(4)).getValue().doubleValue());
    Assert.assertEquals("4th sma", 56.5, ((KeyValPair<String, Double>)sink.collectedTuples.get(6)).getValue().doubleValue());
    Assert.assertEquals("1st sma", 52, ((KeyValPair<String, Integer>)sink2.collectedTuples.get(0)).getValue().intValue());

  }
}
