package com.datatorrent.lib.datamodel.metric;

import java.util.HashMap;

import junit.framework.Assert;

import org.junit.Test;

public class MetricAverageTest
{

  @Test
  public void testIntType()
  {
    HashMap<String, Object> map = new HashMap<String, Object>();
    map.put("value", 1);
    MetricAverage sum = new MetricAverage("value", "count");
    HashMap<String, Object> outputMap = new HashMap<String, Object>();
    sum.aggregate(outputMap, map);
    HashMap<String, Object> map1 = new HashMap<String, Object>();
    map1.put("value", 2);
    sum.aggregate(outputMap, map1);
    Assert.assertEquals(true, outputMap.get("value") instanceof Integer);
    Assert.assertEquals(3, outputMap.get("value"));
    Assert.assertEquals(2, outputMap.get("count"));
    Assert.assertEquals(true, outputMap.get("count") instanceof Integer);
  }

  @Test
  public void testLongType()
  {
    HashMap<String, Object> map = new HashMap<String, Object>();
    map.put("value", 1l);
    MetricAverage sum = new MetricAverage("value", "count");
    HashMap<String, Object> outputMap = new HashMap<String, Object>();
    sum.aggregate(outputMap, map);
    HashMap<String, Object> map1 = new HashMap<String, Object>();
    map1.put("value", 2l);
    sum.aggregate(outputMap, map1);
    Assert.assertEquals(true, outputMap.get("value") instanceof Long);
    Assert.assertEquals(3l, outputMap.get("value"));
    Assert.assertEquals(2, outputMap.get("count"));
    Assert.assertEquals(true, outputMap.get("count") instanceof Integer);

  }

  @Test
  public void testDoubleType()
  {
    HashMap<String, Object> map = new HashMap<String, Object>();
    map.put("value", 1.0);
    MetricAverage sum = new MetricAverage("value", "count");
    HashMap<String, Object> outputMap = new HashMap<String, Object>();
    sum.aggregate(outputMap, map);
    HashMap<String, Object> map1 = new HashMap<String, Object>();
    map1.put("value", 2.0);
    sum.aggregate(outputMap, map1);
    Assert.assertEquals(true, outputMap.get("value") instanceof Double);
    Assert.assertEquals(3.0, outputMap.get("value"));
    Assert.assertEquals(2, outputMap.get("count"));
    Assert.assertEquals(true, outputMap.get("count") instanceof Integer);
  }

  @Test
  public void testFloatType()
  {
    HashMap<String, Object> map = new HashMap<String, Object>();
    map.put("value", 1.0f);
    MetricAverage sum = new MetricAverage("value", "count");
    HashMap<String, Object> outputMap = new HashMap<String, Object>();
    sum.aggregate(outputMap, map);
    HashMap<String, Object> map1 = new HashMap<String, Object>();
    map1.put("value", 2.0f);
    sum.aggregate(outputMap, map1);
    Assert.assertEquals(true, outputMap.get("value") instanceof Float);
    Assert.assertEquals(3.0f, outputMap.get("value"));
    Assert.assertEquals(2, outputMap.get("count"));
    Assert.assertEquals(true, outputMap.get("count") instanceof Integer);
  }
}
