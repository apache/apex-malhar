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
package com.datatorrent.lib.datamodel.metric;

import java.util.HashMap;

import junit.framework.Assert;

import org.junit.Test;

/**
 * Test case for MetricSum
 * 
 */
public class MetricSumTest
{
  @Test
  public void testIntType()
  {
    HashMap<String, Object> map = new HashMap<String, Object>();
    map.put("value", 1);
    MetricSum sum = new MetricSum("value");
    HashMap<String, Object> outputMap = new HashMap<String, Object>();
    sum.aggregate(outputMap, map);
    HashMap<String, Object> map1 = new HashMap<String, Object>();
    map1.put("value", 2);
    sum.aggregate(outputMap, map1);
    Assert.assertEquals(true, outputMap.get("value") instanceof Integer);
    Assert.assertEquals(3, outputMap.get("value"));
  }

  @Test
  public void testLongType()
  {
    HashMap<String, Object> map = new HashMap<String, Object>();
    map.put("value", 1l);
    MetricSum sum = new MetricSum("value");
    HashMap<String, Object> outputMap = new HashMap<String, Object>();
    sum.aggregate(outputMap, map);
    HashMap<String, Object> map1 = new HashMap<String, Object>();
    map1.put("value", 2l);
    sum.aggregate(outputMap, map1);
    Assert.assertEquals(true, outputMap.get("value") instanceof Long);
    Assert.assertEquals(3l, outputMap.get("value"));
  }

  @Test
  public void testDoubleType()
  {
    HashMap<String, Object> map = new HashMap<String, Object>();
    map.put("value", 1.0);
    MetricSum sum = new MetricSum("value");
    HashMap<String, Object> outputMap = new HashMap<String, Object>();
    sum.aggregate(outputMap, map);
    HashMap<String, Object> map1 = new HashMap<String, Object>();
    map1.put("value", 2.0);
    sum.aggregate(outputMap, map1);
    Assert.assertEquals(true, outputMap.get("value") instanceof Double);
    Assert.assertEquals(3.0, outputMap.get("value"));
  }

  @Test
  public void testFloatType()
  {
    HashMap<String, Object> map = new HashMap<String, Object>();
    map.put("value", 1.0f);
    MetricSum sum = new MetricSum("value");
    HashMap<String, Object> outputMap = new HashMap<String, Object>();
    sum.aggregate(outputMap, map);
    HashMap<String, Object> map1 = new HashMap<String, Object>();
    map1.put("value", 2.0f);
    sum.aggregate(outputMap, map1);
    Assert.assertEquals(true, outputMap.get("value") instanceof Float);
    Assert.assertEquals(3.0f, outputMap.get("value"));
  }
}
