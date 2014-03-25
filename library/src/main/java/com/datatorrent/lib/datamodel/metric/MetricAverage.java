package com.datatorrent.lib.datamodel.metric;

import java.util.Map;

import com.google.common.base.Preconditions;

public class MetricAverage implements Metric<Map<String, Object>, Map<String, Object>>
{

  private final String metricKey;
  private final String countKey;

  MetricAverage(String metricKey, String countKey)
  {
    this.metricKey = Preconditions.checkNotNull(metricKey, "metric key");
    this.countKey = Preconditions.checkNotNull(countKey, "count key");
  }

  @Override
  public void aggregate(Map<String, Object> destination, Map<String, Object> event)
  {
    Number sum = (Number) destination.get(metricKey);
    if (sum == null) {
      sum = 0;
    }
    Number x = (Number) event.get(metricKey);
    double newSum = x.doubleValue() + sum.doubleValue();
    if (x instanceof Integer) {
      destination.put(metricKey, (int) newSum);
    } else if (x instanceof Long) {
      destination.put(metricKey, (long) newSum);
    } else if (x instanceof Float) {
      destination.put(metricKey, (float) newSum);
    } else {
      destination.put(metricKey, newSum);
    }

    Integer count = (Integer) destination.get(countKey);
    if (count == null) {
      destination.put(countKey, 1);
    } else {
      destination.put(countKey, ++count);
    }
  }

}
