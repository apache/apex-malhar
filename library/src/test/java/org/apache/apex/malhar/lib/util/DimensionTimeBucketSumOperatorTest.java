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
package org.apache.apex.malhar.lib.util;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.testbench.CollectorTestSink;
import org.apache.commons.lang.mutable.MutableDouble;

import com.google.common.collect.Maps;

@SuppressWarnings({"rawtypes", "unchecked"})
public class DimensionTimeBucketSumOperatorTest
{
  private static Logger logger = LoggerFactory.getLogger(DimensionTimeBucketSumOperatorTest.class);

  private Map<String, Object> getMap(String ipAddr, String url, String status, String agent, int bytes)
  {

    Map<String, Object> map = new HashMap<String, Object>();
    map.put("ipAddr", ipAddr);
    map.put("url", url);
    map.put("status", status);
    map.put("agent", agent);
    map.put("bytes", bytes);

    return map;
  }

  @Test
  public void testDimensionTimeBucket() throws InterruptedException
  {
    DimensionTimeBucketSumOperator oper = new DimensionTimeBucketSumOperator();
    CollectorTestSink sortSink = new CollectorTestSink();
    oper.out.setSink(sortSink);

    oper.addDimensionKeyName("ipAddr");
    oper.addDimensionKeyName("url");
    oper.addDimensionKeyName("status");
    oper.addDimensionKeyName("agent");
    oper.addValueKeyName("bytes");
    Set<String> dimensionKey = new HashSet<String>();

    dimensionKey.add("ipAddr");
    dimensionKey.add("url");
    try {
      oper.addCombination(dimensionKey);
    } catch (NoSuchFieldException e) {
      //ignored
    }
    oper.setTimeBucketFlags(AbstractDimensionTimeBucketOperator.TIMEBUCKET_MINUTE);
    oper.setup(null);
    oper.beginWindow(0);
    oper.in.process(getMap("10.10.1.1", "/movies", "200", "FF", 20));
    oper.in.process(getMap("10.10.1.2", "/movies", "200", "FF", 20));
    oper.in.process(getMap("10.10.1.2", "/movies", "200", "FF", 20));
    oper.endWindow();
    Map<String, Map<String, Number>> outputMap = Maps.newHashMap();
    Map<String, Number> key1 = Maps.newHashMap();
    key1.put("1", new MutableDouble(40.0));
    key1.put("0", new MutableDouble(2.0));
    outputMap.put("m|197001010000|0:10.10.1.2|1:/movies", key1);
    Map<String, Number> key2 = Maps.newHashMap();
    key2.put("0", new MutableDouble(1.0));
    key2.put("1", new MutableDouble(20.0));
    outputMap.put("m|197001010000|0:10.10.1.1|1:/movies", key2);
    Assert.assertEquals("number emitted tuples", 1, sortSink.collectedTuples.size());
    for (Object o : sortSink.collectedTuples) {
      Assert.assertEquals("content of tuple ", outputMap, o);
      logger.debug(o.toString());
    }
    logger.debug("Done testing round\n");
  }

}
