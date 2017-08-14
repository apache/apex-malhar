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
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang.mutable.MutableDouble;

/**
 * This operator accumulates the values of "value" fields for different time and dimensions and emits the accumulated values as a map.&nbsp;
 * The emitted map's keys are a combination of the time and dimension fields,
 * and the emitted map's values are another map from value fields to and accumulated value.
 * <p></p>
 * @displayName Dimension Time Bucket Sum
 * @category Stats and Aggregations
 * @tags count, key value, numeric
 * @since 0.3.2
 */
public class DimensionTimeBucketSumOperator extends AbstractDimensionTimeBucketOperator
{
  private static final Logger LOG = LoggerFactory.getLogger(DimensionTimeBucketSumOperator.class);
  private Map<String, Map<String, Number>> dataMap;

  @Override
  public void process(String timeBucket, String key, String field, Number value)
  {
    String finalKey = timeBucket + "|" + key;
    Map<String, Number> m = dataMap.get(finalKey);
    if (value == null) {
      return;
    }
    if (m == null) {
      m = new HashMap<String, Number>();
      m.put(field, new MutableDouble(value));
      dataMap.put(finalKey, m);
    } else {
      Number n = m.get(field);
      if (n == null) {
        m.put(field, new MutableDouble(value));
      } else {
        ((MutableDouble)n).add(value);
      }
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
    if (!dataMap.isEmpty()) {
      out.emit(dataMap);
      LOG.info("Number of keyval pairs: {}", dataMap.size());
    }
  }

}
