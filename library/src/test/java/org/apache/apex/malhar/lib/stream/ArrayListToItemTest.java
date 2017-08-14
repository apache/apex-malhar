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
package org.apache.apex.malhar.lib.stream;

import java.util.ArrayList;

import org.junit.Assert;
import org.junit.Test;

import org.apache.apex.malhar.lib.testbench.CountTestSink;

/**
 * Performance test for {@link org.apache.apex.malhar.lib.stream.StreamDuplicater}<p>
 * Benchmarks: Currently does about ?? Million tuples/sec in debugging environment. Need to test on larger nodes<br>
 * <br>
 */
public class ArrayListToItemTest
{
  /**
   * Test operator pass through. The Object passed is not relevant
   */
  @SuppressWarnings({"rawtypes", "unchecked"})
  @Test
  public void testNodeProcessing() throws Exception
  {
    ArrayListToItem oper = new ArrayListToItem();
    CountTestSink itemSink = new CountTestSink();
    oper.item.setSink(itemSink);

    oper.beginWindow(0);
    ArrayList<String> input = new ArrayList<String>();
    input.add("a");
    // Same input object can be used as the oper is just pass through
    int numtuples = 1000;
    for (int i = 0; i < numtuples; i++) {
      oper.data.process(input);
    }

    oper.endWindow();
    Assert.assertEquals("number emitted tuples", numtuples, itemSink.count);
  }
}
