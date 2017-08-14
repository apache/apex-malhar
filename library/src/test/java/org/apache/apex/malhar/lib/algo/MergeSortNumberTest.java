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
package org.apache.apex.malhar.lib.algo;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Random;

import org.junit.Test;

import org.apache.apex.malhar.lib.testbench.CollectorTestSink;

import static org.junit.Assert.assertTrue;

/**
 * @deprecated
 * Functional tests for {@link org.apache.apex.malhar.lib.algo.MergeSort}<p>
 *   (Deprecating inclass) Comment: MergeSortNumber is deprecated.
 */
@Deprecated
public class MergeSortNumberTest
{
  /**
   * Test node logic emits correct results
   */
  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Test
  public void testNodeProcessing() throws Exception
  {
    MergeSortNumber<Integer> oper = new MergeSortNumber<Integer>();
    CollectorTestSink sink = new CollectorTestSink();
    oper.sort.setSink(sink);

    oper.setup(null);
    oper.beginWindow(1);

    Random rand = new Random();
    ArrayList<Integer> tuple = new ArrayList<Integer>();
    tuple.add(rand.nextInt(50));
    tuple.add(50 + rand.nextInt(50));
    oper.process(tuple);
    tuple = new ArrayList<Integer>();
    tuple.add(rand.nextInt(50));
    tuple.add(50 + rand.nextInt(50));
    oper.process(tuple);

    oper.endWindow();
    oper.teardown();

    assertTrue("Tuples in sink", sink.collectedTuples.size() == 1);
    Iterator iter = sink.collectedTuples.iterator();
    if (!iter.hasNext()) {
      return;
    }
    tuple = (ArrayList<Integer>)iter.next();
    assertTrue("Tuple size 4", tuple.size() == 4);
    Integer val = tuple.get(0);
    for (int i = 1; i < 4; i++) {
      assertTrue("Values must be sorted " + tuple, val <= tuple.get(i));
      val = tuple.get(i);
    }
  }
}
