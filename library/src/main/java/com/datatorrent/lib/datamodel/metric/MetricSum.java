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

import java.util.Map;

import com.google.common.base.Preconditions;

public class MetricSum implements Metric<Map<String, Object>, Map<String, Object>>
{

  private final String metricKey;

  public MetricSum(String metricKey)
  {
    this.metricKey = Preconditions.checkNotNull(metricKey, "metric check");
  }

  private MetricSum()
  {
    // used for kryo serializaiton
    this.metricKey = null;
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
  }

}
