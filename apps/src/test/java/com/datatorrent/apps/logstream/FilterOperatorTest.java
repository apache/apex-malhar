/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
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
package com.datatorrent.apps.logstream;

import java.util.*;
import org.junit.Assert;

import org.junit.Test;

import com.datatorrent.lib.testbench.CollectorTestSink;

import com.datatorrent.apps.logstream.PropertyRegistry.LogstreamPropertyRegistry;

/**
 *
 * Tests logstream filter operator.
 */
public class FilterOperatorTest
{
  @Test
  @SuppressWarnings("unchecked")
  public void testOperator()
  {
    FilterOperator oper = new FilterOperator();
    LogstreamPropertyRegistry registry = new LogstreamPropertyRegistry();
    registry.bind(LogstreamUtil.LOG_TYPE, "apache");
    oper.setRegistry(registry);
    oper.setup(null);

    String filter1 = "a==\"1\"&&b==\"2\"&&c_info==\"abc\"";
    oper.addFilterCondition(new String[] {"type=apache", "a", "b", "c_info", "d", filter1});
    String filter2 = "d==1";
    oper.addFilterCondition(new String[] {"type=apache", "d", filter2});
    String filter3 = "a==\"1\"";
    oper.addFilterCondition(new String[] {"type=apache", "a", filter3});
    String filter4 = "e==\"2\"";
    oper.addFilterCondition(new String[] {"type=apache", "e", filter4});
    String filter5 = "response.equals(\"404\")";
    oper.addFilterCondition(new String[] {"type=apache", "response", filter5});
    String filter6 = "default=true";
    oper.addFilterCondition(new String[] {"type=apache", filter6});
    HashMap<String, Object> inMap = new HashMap<String, Object>();

    inMap.put(LogstreamUtil.LOG_TYPE, registry.getIndex(LogstreamUtil.LOG_TYPE, "apache"));
    inMap.put("a", "1");
    inMap.put("b", "2");
    inMap.put("c_info", "abc");
    inMap.put("d", 1);
    inMap.put("e", "3");
    inMap.put("response", "404");

    Set<Integer> expectedPassSet = new HashSet<Integer>();
    expectedPassSet.add(registry.getIndex(LogstreamUtil.FILTER, filter1));
    expectedPassSet.add(registry.getIndex(LogstreamUtil.FILTER, filter2));
    expectedPassSet.add(registry.getIndex(LogstreamUtil.FILTER, filter3));
    expectedPassSet.add(registry.getIndex(LogstreamUtil.FILTER, filter5));
    expectedPassSet.add(registry.getIndex(LogstreamUtil.FILTER, "apache_DEFAULT"));

    CollectorTestSink mapSink = new CollectorTestSink();
    oper.outputMap.setSink(mapSink);

    oper.beginWindow(0);
    oper.input.process(inMap);
    oper.endWindow();

    Assert.assertEquals("tuple count", 5, mapSink.collectedTuples.size());

    List<HashMap<String, Object>> tuples = mapSink.collectedTuples;

    Set<Integer> actualPassSet = new HashSet<Integer>();
    for (HashMap<String, Object> tuple : tuples) {
      Integer filter = (Integer)tuple.get(LogstreamUtil.FILTER);
      actualPassSet.add(filter);
    }

    Assert.assertEquals("Passed filters", expectedPassSet, actualPassSet);
  }

}
