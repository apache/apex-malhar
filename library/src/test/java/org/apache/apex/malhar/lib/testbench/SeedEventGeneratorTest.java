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

import com.datatorrent.api.Operator.ShutdownException;

import static org.junit.Assert.assertTrue;

/**
 *
 * Functional test for {@link org.apache.apex.malhar.lib.testbench.SeedEventGenerator}<p>
 * <br>
 * Four keys are sent in at a high throughput rate and the classification is expected to be cover all combinations<br>
 * <br>
 * Benchmarks: A total of 40 million tuples are pushed in each benchmark<br>
 * String schema does about 1.5 Million tuples/sec<br>
 * SeedEventGenerator.valueData schema is about 4 Million tuples/sec<br>
 * <br>
 * DRC checks are validated<br>
 */
public class SeedEventGeneratorTest
{
  /**
   * Test node logic emits correct results
   */
  @Test
  public void testNodeProcessing() throws Exception
  {
    testSchemaNodeProcessing(true);
    testSchemaNodeProcessing(false);
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  public void testSchemaNodeProcessing(boolean doseedkey) throws Exception
  {
    SeedEventGenerator node = new SeedEventGenerator();
    if (doseedkey) {
      node.addKeyData("x", 0, 9);
      node.addKeyData("y", 0, 9);
      node.addKeyData("gender", 0, 1);
      node.addKeyData("age", 10, 19);
    }
    CollectorTestSink keyvalpair_data = new CollectorTestSink();
    node.keyvalpair_list.setSink(keyvalpair_data);

    node.setup(null);
    node.beginWindow(1);
    try {
      node.emitTuples();
    } catch (ShutdownException re) {
      // this one is expected!
    }
    node.endWindow();
    node.teardown();

    assertTrue("Collected tuples", keyvalpair_data.collectedTuples.size() == 99);
  }
}
