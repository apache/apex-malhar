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
package org.apache.apex.malhar.lib.testbench;

import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * Functional test for {@link org.apache.apex.malhar.lib.testbench.RandomEventGenerator}<p>
 * <br>
 * Tests both string and integer. Sets range to 0 to 999 and generates random numbers. With millions
 * of tuple all the values are covered<br>
 * <br>
 * Benchmark: pushes as many tuples are possible<br>
 * String schema does about 3 Million tuples/sec<br>
 * Integer schema does about 7 Million tuples/sec<br>
 * <br>
 * DRC validation is done<br>
 * <br>
 */
public class RandomEventGeneratorTest
{
  static int icount = 0;
  static int imax = -1;
  static int imin = -1;
  static int scount = 0;
  static int smax = -1;
  static int smin = -1;

  protected void clear()
  {
    icount = 0;
    imax = -1;
    imin = -1;
    scount = 0;
    smax = -1;
    smin = -1;
  }

  /**
   * Test node logic emits correct results
   */
  @Test
  public void testNodeProcessing() throws Exception
  {
    testSchemaNodeProcessing();
    testSchemaNodeProcessing();
    testSchemaNodeProcessing();
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  public void testSchemaNodeProcessing() throws Exception
  {
    RandomEventGenerator node = new RandomEventGenerator();
    node.setMinvalue(0);
    node.setMaxvalue(999);
    node.setTuplesBlast(5000);
    CollectorTestSink integer_data = new CollectorTestSink();
    node.integer_data.setSink(integer_data);
    CollectorTestSink string_data = new CollectorTestSink();
    node.string_data.setSink(string_data);

    node.setup(null);
    node.beginWindow(1);
    node.emitTuples();
    node.endWindow();
    node.teardown();
    assertTrue("tuple blast", integer_data.collectedTuples.size() == 5000);
    assertTrue("tuple blast", string_data.collectedTuples.size() == 5000);
  }
}
