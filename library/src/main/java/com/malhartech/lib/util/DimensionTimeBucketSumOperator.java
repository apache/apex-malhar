/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.util;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import org.apache.commons.lang.mutable.MutableDouble;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author David Yan <davidyan@malhar-inc.com>
 */
public class DimensionTimeBucketSumOperator extends DimensionTimeBucketOperator
{
  private static final Logger LOG = LoggerFactory.getLogger(DimensionTimeBucketSumOperator.class);
  private Map<String, Map<String, Map<String, Number>>> dataMap;

  @Override
  public void process(String timeBucket, String key, String field, Number value)
  {
    Map<String, Map<String, Number>> m1 = dataMap.get(timeBucket);
    if (m1 == null) {
      m1 = new TreeMap<String, Map<String, Number>>();
      dataMap.put(timeBucket, m1);
    }
    Map<String, Number> m2 = m1.get(key);
    if (value == null) {
      return;
    }
    if (m2 == null) {
      m2 = new HashMap<String, Number>();
      m2.put(field, new MutableDouble(value));
      m1.put(key, m2);
    }
    else {
      Number n = m2.get(field);
      if (n == null) {
        m2.put(field, new MutableDouble(value));
      } else {
        ((MutableDouble)n).add(value);
      }
    }
  }

  @Override
  public void beginWindow(long windowId)
  {
    super.beginWindow(windowId);
    dataMap = new TreeMap<String, Map<String, Map<String, Number>>>();
  }

  @Override
  public void endWindow()
  {
    if (!dataMap.isEmpty()) {
      out.emit(dataMap);
      LOG.info("Number of time buckets: {}", dataMap.size());
      for (Map.Entry<String, Map<String, Map<String, Number>>> entry : dataMap.entrySet()) {
        LOG.info("Number of keyval pairs for {}: {}", entry.getKey(), entry.getValue().size());
      }
    }
  }

}
