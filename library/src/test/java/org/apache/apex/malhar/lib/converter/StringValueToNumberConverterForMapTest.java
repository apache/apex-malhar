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
package org.apache.apex.malhar.lib.converter;

import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import org.apache.apex.malhar.lib.testbench.CollectorTestSink;
import org.apache.apex.malhar.lib.util.TestUtils;

public class StringValueToNumberConverterForMapTest
{

  @Test
  public void testStringValueToNumericConversion()
  {
    StringValueToNumberConverterForMap<String> testop = new StringValueToNumberConverterForMap<String>();
    String[] values = {"1.0", "2.0", "3.0"};
    String[] keys = {"a", "b", "c"};

    HashMap<String, String> inputMap = new HashMap<String, String>();

    for (int i = 0; i < 3; i++) {
      inputMap.put(keys[i], values[i]);
    }

    CollectorTestSink<Map<String, Number>> testsink = new CollectorTestSink<Map<String, Number>>();
    TestUtils.setSink(testop.output, testsink);

    testop.beginWindow(0);

    testop.input.put(inputMap);

    testop.endWindow();

    Assert.assertEquals(1,testsink.collectedTuples.size());

    Map<String, Number> output = testsink.collectedTuples.get(0);

    Assert.assertEquals(output.get("a"), 1.0);
    Assert.assertEquals(output.get("b"), 2.0);
    Assert.assertEquals(output.get("c"), 3.0);

  }
}
