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
package org.apache.apex.malhar.contrib.misc.math;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.testbench.CollectorTestSink;
import org.apache.apex.malhar.lib.util.KeyValPair;

/**
 *
 * Functional tests for {@link ChangeKeyVal}.
 * <p>
 * @deprecated
 */
@Deprecated
public class ChangeKeyValTest
{
  private static Logger log = LoggerFactory.getLogger(ChangeKeyValTest.class);

  /**
   * Test node logic emits correct results.
   */
  @Test
  public void testNodeProcessing() throws Exception
  {
    testNodeProcessingSchema(new ChangeKeyVal<String, Integer>());
    testNodeProcessingSchema(new ChangeKeyVal<String, Double>());
    testNodeProcessingSchema(new ChangeKeyVal<String, Float>());
    testNodeProcessingSchema(new ChangeKeyVal<String, Short>());
    testNodeProcessingSchema(new ChangeKeyVal<String, Long>());
  }

  /**
   *
   * @param oper
   *          key/value pair for comparison.
   */
  @SuppressWarnings({ "rawtypes", "unchecked" })
  public <V extends Number> void testNodeProcessingSchema(
      ChangeKeyVal<String, V> oper)
  {
    CollectorTestSink changeSink = new CollectorTestSink();
    CollectorTestSink percentSink = new CollectorTestSink();

    oper.change.setSink(changeSink);
    oper.percent.setSink(percentSink);

    oper.beginWindow(0);
    oper.base.process(new KeyValPair<String, V>("a", oper.getValue(2)));
    oper.base.process(new KeyValPair<String, V>("b", oper.getValue(10)));
    oper.base.process(new KeyValPair<String, V>("c", oper.getValue(100)));

    oper.data.process(new KeyValPair<String, V>("a", oper.getValue(3)));
    oper.data.process(new KeyValPair<String, V>("b", oper.getValue(2)));
    oper.data.process(new KeyValPair<String, V>("c", oper.getValue(4)));

    oper.endWindow();

    // One for each key
    Assert.assertEquals("number emitted tuples", 3,
        changeSink.collectedTuples.size());
    Assert.assertEquals("number emitted tuples", 3,
        percentSink.collectedTuples.size());

    log.debug("\nLogging tuples");
    for (Object o : changeSink.collectedTuples) {
      KeyValPair<String, Number> kv = (KeyValPair<String, Number>)o;
      if (kv.getKey().equals("a")) {
        Assert.assertEquals("change in a ", 1.0, kv.getValue());
      }
      if (kv.getKey().equals("b")) {
        Assert.assertEquals("change in b ", -8.0, kv.getValue());
      }
      if (kv.getKey().equals("c")) {
        Assert.assertEquals("change in c ", -96.0, kv.getValue());
      }
    }

    for (Object o : percentSink.collectedTuples) {
      KeyValPair<String, Number> kv = (KeyValPair<String, Number>)o;
      if (kv.getKey().equals("a")) {
        Assert.assertEquals("change in a ", 50.0, kv.getValue());
      }
      if (kv.getKey().equals("b")) {
        Assert.assertEquals("change in b ", -80.0, kv.getValue());
      }
      if (kv.getKey().equals("c")) {
        Assert.assertEquals("change in c ", -96.0, kv.getValue());
      }
    }
  }
}
