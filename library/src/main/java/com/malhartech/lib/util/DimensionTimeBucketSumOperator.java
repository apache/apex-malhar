/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.util;

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
  private Map<String, Map<String, Number>> dataMap;

  @Override
  public void process(String timeBucket, String key, Number value)
  {
    Map<String, Number> m = dataMap.get(timeBucket);
    if (m == null) {
      m = new TreeMap<String, Number>();
      dataMap.put(timeBucket, m);
    }
    Number n = m.get(key);
    if (value == null) {
      return;
    }
    if (n == null) {
      n = new MutableDouble(value);
      m.put(key, n);
    }
    else {
      ((MutableDouble)n).add(value);
    }
  }

  @Override
  public void beginWindow(long windowId)
  {
    super.beginWindow(windowId);
    dataMap = new TreeMap<String, Map<String, Number>>();
  }

  @Override
  public void endWindow()
  {
    if (!dataMap.isEmpty()) {
      out.emit(dataMap);
      LOG.info("Number of time buckets: {}", dataMap.size());
      for (Map.Entry<String, Map<String, Number>> entry : dataMap.entrySet()) {
        LOG.info("Number of keyval pairs for {}: {}", entry.getKey(), entry.getValue().size());
      }
    }
  }

}
