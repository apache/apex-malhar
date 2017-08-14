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

import java.util.Set;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.testbench.CollectorTestSink;
import org.apache.apex.malhar.lib.util.KeyValPair;
import org.apache.apex.malhar.lib.util.TestUtils;

import com.google.common.collect.ImmutableMap;

/**
 * Test for {@link org.apache.apex.malhar.lib.algo.UniqueValueCount} operator
 *
 * @since 0.3.5
 */
public class UniqueValueCountTest
{
  private static Logger LOG = LoggerFactory.getLogger(UniqueValueCountTest.class);

  @Test
  public void uniqueCountTest()
  {
    UniqueValueCount<String> uniqueCountOper = new UniqueValueCount<String>();
    CollectorTestSink<KeyValPair<String, Integer>> outputSink =
        new CollectorTestSink<KeyValPair<String, Integer>>();
    CollectorTestSink<KeyValPair<String, Set<Object>>> outputSetSink =
        new CollectorTestSink<KeyValPair<String, Set<Object>>>();
    TestUtils.setSink(uniqueCountOper.output, outputSink);
    TestUtils.setSink(uniqueCountOper.outputValues, outputSetSink);

    uniqueCountOper.beginWindow(0);
    uniqueCountOper.input.process(new KeyValPair<String, Object>("test1", 1));
    uniqueCountOper.input.process(new KeyValPair<String, Object>("test1", 2));
    uniqueCountOper.input.process(new KeyValPair<String, Object>("test1", 2));
    uniqueCountOper.endWindow();

    Assert.assertEquals("number emitted tuples", 1, outputSink.collectedTuples.size());
    KeyValPair<String, Integer> emittedPair = outputSink.collectedTuples.get(0);

    Assert.assertEquals("emitted key was ", "test1", emittedPair.getKey());
    Assert.assertEquals("emitted value was ", 2, emittedPair.getValue().intValue());

    Assert.assertEquals("number emitted tuples", 1, outputSetSink.collectedTuples.size());
    KeyValPair<String, Set<Object>> emittedSetPair = outputSetSink.collectedTuples.get(0);
    Assert.assertTrue(emittedSetPair.getValue().contains(1));
    Assert.assertTrue(emittedSetPair.getValue().contains(2));

    outputSink.clear();
    uniqueCountOper.beginWindow(1);
    uniqueCountOper.input.process(new KeyValPair<String, Object>("test1", 1));
    uniqueCountOper.input.process(new KeyValPair<String, Object>("test1", 2));
    uniqueCountOper.input.process(new KeyValPair<String, Object>("test1", 2));
    uniqueCountOper.input.process(new KeyValPair<String, Object>("test2", 1));
    uniqueCountOper.input.process(new KeyValPair<String, Object>("test2", 2));
    uniqueCountOper.input.process(new KeyValPair<String, Object>("test2", 2));
    uniqueCountOper.endWindow();

    ImmutableMap<String, Integer> answers = ImmutableMap.of("test1", 2, "test2", 2);

    Assert.assertEquals("number emitted tuples", 2, outputSink.collectedTuples.size());
    for (KeyValPair<String, Integer> emittedPair2 : outputSink.collectedTuples) {
      Assert.assertEquals("emmit value of " + emittedPair2.getKey() + " was ", answers.get(emittedPair2.getKey()),
          emittedPair2.getValue());
    }
    LOG.debug("Done unique count testing testing\n");
  }

}
