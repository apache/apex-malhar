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
 * limitations under the License.
 */
package com.datatorrent.lib.algo;

import com.datatorrent.engine.TestSink;
import com.datatorrent.lib.algo.UniqueValueKeyVal;
import com.datatorrent.lib.util.KeyValPair;
import junit.framework.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Performance tests for {@link com.datatorrent.lib.algo.UniqueValueKeyVal}<p>
 *
 */
public class UniqueValueKeyValBenchmark
{
  private static Logger log = LoggerFactory.getLogger(UniqueValueKeyValBenchmark.class);

  /**
   * Test node logic emits correct results
   */
  @Test
  @SuppressWarnings("SleepWhileInLoop")
  @Category(com.datatorrent.annotation.PerformanceTestCategory.class)
  public void testNodeProcessing() throws Exception
  {
    UniqueValueKeyVal<String> oper = new UniqueValueKeyVal<String>();
    TestSink sink = new TestSink();
    oper.count.setSink(sink);

    KeyValPair<String,Integer> atuple = new KeyValPair("a", 1);
    KeyValPair<String,Integer> btuple = new KeyValPair("b", 1);
    KeyValPair<String,Integer> ctuple = new KeyValPair("c", 5);
    KeyValPair<String,Integer> dtuple = new KeyValPair("d", 2);
    KeyValPair<String,Integer> e1tuple = new KeyValPair("e", 5);
    KeyValPair<String,Integer> e2tuple = new KeyValPair("e", 2);

    int numTuples = 10000000;
    oper.beginWindow(0);
    for (int i = 0; i < numTuples; i++) {
      atuple.setValue(i);
      oper.data.process(atuple);
      if (i % 2 == 0) {
        oper.data.process(btuple);
        oper.data.process(e2tuple);
      }
      if (i % 3 == 0) {
        oper.data.process(ctuple);
      }
      if (i % 5 == 0) {
        oper.data.process(dtuple);
      }
      if (i % 10 == 0) {
        oper.data.process(e1tuple);
      }
    }
    oper.endWindow();
    Assert.assertEquals("number emitted tuples", 5, sink.collectedTuples.size());
    log.debug(String.format("\nBenchmark sums for %d key/val pairs", numTuples * 6));

    for (Object o : sink.collectedTuples) {
      KeyValPair<String,Integer> e = (KeyValPair<String,Integer>)o;
      int val = e.getValue().intValue();
      if (e.getKey().equals("a")) {
        Assert.assertEquals("emitted value for 'a' was ", numTuples, val);
      }
      else if (e.getKey().equals("b")) {
        Assert.assertEquals("emitted tuple for 'b' was ", 1, val);
      }
      else if (e.getKey().equals("c")) {
        Assert.assertEquals("emitted tuple for 'c' was ", 1, val);
      }
      else if (e.getKey().equals("d")) {
        Assert.assertEquals("emitted tuple for 'd' was ", 1, val);
      }
      else if (e.getKey().equals("e")) {
        Assert.assertEquals("emitted tuple for 'e' was ", 2, val);
      }
    }
  }
}
