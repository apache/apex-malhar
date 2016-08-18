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
package com.datatorrent.lib.streamquery;

import com.datatorrent.lib.streamquery.function.NvlFunction;
import com.datatorrent.lib.testbench.CollectorTestSink;
import org.junit.Assert;
import org.junit.Test;

/**
 * Functional tests for NvlFunction
 *
 */
public class NvlFunctionTest
{
  /**
   * Test operator logic emits correct results.
   * Data type: String
   */
  @SuppressWarnings({"rawtypes", "unchecked"})
  @Test
  public void testNodeProcessingString()
  {
    NvlFunction<String> strOper = new NvlFunction<String>();

    CollectorTestSink strSink = new CollectorTestSink();
    CollectorTestSink eSink = new CollectorTestSink();

    strOper.out.setSink(strSink);
    strOper.errordata.setSink(eSink);

    strOper.beginWindow(0);

    // positive case: nvl("abc", "default addr") --> "abc"
    strOper.column.process("abc");
    strOper.alias.process("default addr");

    // positive case: nvl(null, "default addr") --> "default addr"
    strOper.column.process(null);
    strOper.alias.process("default addr");

    // negative case: nvl("abc", null) --> error
    strOper.column.process("abc");
    strOper.alias.process(null);

    strOper.endWindow();

    // Two positive results, one error
    Assert.assertEquals("number of emitted tuples", 2,
        strSink.collectedTuples.size());
    Assert.assertEquals("NVL is", "abc",
            strSink.collectedTuples.get(0));
    Assert.assertEquals("NVL is", "default addr",
            strSink.collectedTuples.get(1));

    Assert.assertEquals("number of error tuples", 1,
        eSink.collectedTuples.size());
  }

  /**
   * Test operator logic: throws exception when number of column values
   * doesn't match number of aliases
   */
  @SuppressWarnings({"rawtypes", "unchecked"})
  @Test
  public void testNodeProcessingException()
  {
    NvlFunction<String> strOper = new NvlFunction<String>();
    Throwable e = null;

    CollectorTestSink strSink = new CollectorTestSink();
    CollectorTestSink eSink = new CollectorTestSink();
    strOper.out.setSink(strSink);
    strOper.errordata.setSink(eSink);

    // negative case: nvl("abc", --) i.e. no alias input --> exception
    try {
      strOper.beginWindow(0);
      strOper.column.process("abc");
      strOper.endWindow();
    }
    catch (Throwable ex) {
      e = ex;
    }

    Assert.assertTrue(e instanceof IllegalArgumentException);
  }

  /**
   * Test operator logic emits correct results.
   * Data type: Integer
   */
  @SuppressWarnings({"rawtypes", "unchecked"})
  @Test
  public void testNodeProcessingInterger()
  {
    NvlFunction<Integer> intOper = new NvlFunction<Integer>();
    CollectorTestSink intSink = new CollectorTestSink();
    CollectorTestSink eSink = new CollectorTestSink();

    intOper.out.setSink(intSink);
    intOper.errordata.setSink(eSink);

    intOper.beginWindow(0);

    // positive case: nvl(123, 0) --> 123
    intOper.column.process(123);
    intOper.alias.process(0);

    // positive case: nvl(null, 0) --> 0
    intOper.column.process(null);
    intOper.alias.process(0);

    // negative case: nvl(123, null) --> error
    intOper.column.process(123);
    intOper.alias.process(null);
    intOper.endWindow();

    // Two positive results, one error
    Assert.assertEquals("number of emitted tuples", 2,
            intSink.collectedTuples.size());
    Assert.assertEquals("NVL is", 123,
            intSink.collectedTuples.get(0));
    Assert.assertEquals("NVL is", 0,
            intSink.collectedTuples.get(1));

    Assert.assertEquals("number of error tuples", 1,
            eSink.collectedTuples.size());
  }

  /**
   * Test operator logic emits correct results.
   * Data type: Double
   */
  @SuppressWarnings({"rawtypes", "unchecked"})
  @Test
  public void testNodeProcessingFloat()
  {
    NvlFunction<Double> doubleOper = new NvlFunction<Double>();
    CollectorTestSink doubleSink = new CollectorTestSink();
    CollectorTestSink eSink = new CollectorTestSink();

    doubleOper.out.setSink(doubleSink);
    doubleOper.errordata.setSink(eSink);

    doubleOper.beginWindow(0);

    // positive case: nvl(3.14, 0) --> 3.14
    doubleOper.column.process(3.14);
    doubleOper.alias.process(0.0);

    // positive case: nvl(null, 0.0) --> 0.0
    doubleOper.column.process(null);
    doubleOper.alias.process(0.0);

    // negative case: nvl(3.14, null) --> error
    doubleOper.column.process(3.14);
    doubleOper.alias.process(null);
    doubleOper.endWindow();

    // Two positive results, one error
    Assert.assertEquals("number of emitted tuples", 2,
            doubleSink.collectedTuples.size());
    Assert.assertEquals("NVL is", 3.14,
            doubleSink.collectedTuples.get(0));
    Assert.assertEquals("NVL is", 0.0,
            doubleSink.collectedTuples.get(1));

    Assert.assertEquals("number of error tuples", 1,
            eSink.collectedTuples.size());
  }
}
