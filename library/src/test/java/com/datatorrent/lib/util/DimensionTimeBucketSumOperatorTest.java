/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.lib.util;

import com.datatorrent.lib.testbench.CollectorTestSink;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.mutable.MutableDouble;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    }
    catch (NoSuchFieldException e) {
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
