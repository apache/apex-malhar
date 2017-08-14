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
package org.apache.apex.malhar.contrib.misc.algo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.testbench.CollectorTestSink;

import com.datatorrent.api.Sink;

/**
 * @deprecated
 * Functional tests for {@link InvertIndex} <p>
 *
 */
@Deprecated
public class InvertIndexTest
{
  private static Logger log = LoggerFactory.getLogger(InvertIndexTest.class);

  /**
   * Test oper logic emits correct results
   */
  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Test
  public void testNodeProcessing() throws Exception
  {
    InvertIndex<String,String> oper = new InvertIndex<String,String>();
    CollectorTestSink indexSink = new CollectorTestSink();

    Sink inSink = oper.data.getSink();
    oper.index.setSink(indexSink);

    oper.beginWindow(0);

    HashMap<String, String> input = new HashMap<String, String>();

    input.put("a", "str");
    input.put("b", "str");
    inSink.put(input);

    input.clear();
    input.put("a", "str1");
    input.put("b", "str1");
    inSink.put(input);

    input.clear();
    input.put("c", "blah");
    inSink.put(input);

    input.clear();
    input.put("c", "str1");
    inSink.put(input);
    oper.endWindow();

    Assert.assertEquals("number emitted tuples", 3, indexSink.collectedTuples.size());
    for (Object o: indexSink.collectedTuples) {
      log.debug(o.toString());
      HashMap<String, ArrayList<String>> output = (HashMap<String, ArrayList<String>>)o;
      for (Map.Entry<String, ArrayList<String>> e: output.entrySet()) {
        String key = e.getKey();
        ArrayList<String> alist = e.getValue();
        if (key.equals("str1")) {
          Assert.assertEquals("Index for \"str1\" contains \"a\"", true, alist.contains("a"));
          Assert.assertEquals("Index for \"str1\" contains \"b\"", true, alist.contains("b"));
          Assert.assertEquals("Index for \"str1\" contains \"c\"", true, alist.contains("c"));

        } else if (key.equals("str")) {
          Assert.assertEquals("Index for \"str1\" contains \"a\"", true, alist.contains("a"));
          Assert.assertEquals("Index for \"str1\" contains \"b\"", true, alist.contains("b"));
          Assert.assertEquals("Index for \"str1\" contains \"c\"", false, alist.contains("c"));

        } else if (key.equals("blah")) {
          Assert.assertEquals("Index for \"str1\" contains \"a\"", false, alist.contains("a"));
          Assert.assertEquals("Index for \"str1\" contains \"b\"", false, alist.contains("b"));
          Assert.assertEquals("Index for \"str1\" contains \"c\"", true, alist.contains("c"));
        }
      }
    }
  }
}
