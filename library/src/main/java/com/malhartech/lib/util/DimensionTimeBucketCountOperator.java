/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.util;

import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang.mutable.MutableLong;

/**
 *
 * @author David Yan <davidyan@malhar-inc.com>
 */
public class DimensionTimeBucketCountOperator extends DimensionTimeBucketOperator
{
  private Map<String, Map<String, Number>> dataMap;

  @Override
  public void process(String timeBucket, String key, Number value)
  {
    Map<String, Number> m = dataMap.get(timeBucket);
    if (m == null) {
      m = new HashMap<String, Number>();
      dataMap.put(timeBucket, m);
    }
    Number n = m.get(key);
    if (value == null) {
      value = 1;
    }
    if (n == null) {
      n = new MutableLong(value);
      m.put(key, n);
    } else {
      ((MutableLong)n).add(value);
    }
  }

  @Override
  public void beginWindow(long windowId)
  {
    super.beginWindow(windowId);
    dataMap = new HashMap<String, Map<String, Number>>();
  }

  @Override
  public void endWindow()
  {
    out.emit(dataMap);
  }
  
}
