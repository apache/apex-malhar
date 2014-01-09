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

import junit.framework.Assert;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class DimensionTimeBucketSumOperatorTest
{
  private static Logger logger = LoggerFactory.getLogger(DimensionTimeBucketSumOperatorTest.class);
  
  private Map<String,Object> getMap(String ipAddr,String url, String status, String agent,int bytes){

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
    }
    oper.setTimeBucketFlags(AbstractDimensionTimeBucketOperator.TIMEBUCKET_MINUTE);
    oper.setup(null);
    oper.beginWindow(0);
    oper.in.process(getMap("10.10.1.1","/movies","200","FF",20));
    oper.in.process(getMap("10.10.1.2","/movies","200","FF",20));
    oper.in.process(getMap("10.10.1.2","/movies","200","FF",20));
    oper.endWindow();
    
    Assert.assertEquals("number emitted tuples", 1, sortSink.collectedTuples.size());
    for (Object o : sortSink.collectedTuples) {
      Assert.assertEquals("content of tuple ", "{m|197001010000|0:10.10.1.2|1:/movies={1=40.0, 0=2.0}, m|197001010000|0:10.10.1.1|1:/movies={1=20.0, 0=1.0}}", o.toString());
      logger.debug(o.toString());
    }
    logger.debug("Done testing round\n");
  }

}
