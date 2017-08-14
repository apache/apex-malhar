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
package com.datatorrent.apps.logstream;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.apex.malhar.lib.logs.DimensionObject;
import org.apache.apex.malhar.lib.util.KeyValPair;

import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;

/**
 * Operator that takes in aggregations and converts them to format accepted by redis output operator
 *
 * @param <K> Key of aggregation
 * @param <V> Value of aggregation
 * @since 0.9.4
 */
public class AggregationsToRedisOperator<K, V> extends BaseOperator
{
  /**
   * map used to map dimension keys to redis key index
   * key --> dimension key set eg: "2,4"
   * value --> keyIndex eg: "1"
   */
  private HashMap<String, Integer> dimensionToKeyIndexMap;
  private Integer keyIndex;

  public void setKeyIndex(Integer keyIndex)
  {
    this.keyIndex = keyIndex;
  }

  public void setDimensionToDbIndexMap(HashMap<String, Integer> dimensionTodbIndexMap)
  {
    this.dimensionToKeyIndexMap = dimensionTodbIndexMap;
  }

  @InputPortFieldAnnotation(optional=true)
  public final transient DefaultInputPort<HashMap<K, ArrayList<DimensionObject<String>>>> multiWindowDimensionInput = new DefaultInputPort<HashMap<K, ArrayList<DimensionObject<String>>>>()
  {
    @Override
    public void process(HashMap<K, ArrayList<DimensionObject<String>>> tuple)
    {
      for (K dimensionKey : tuple.keySet()) {
        Integer keyInd = dimensionToKeyIndexMap.get(dimensionKey);
        if (keyInd != null) {
          ArrayList<DimensionObject<String>> topList = tuple.get(dimensionKey);
          int numOuts = 0;
          for (DimensionObject<String> item : topList) {
            Map<String, String> out = new HashMap<String, String>();
            String key = new StringBuilder(keyInd.toString()).append("##").append(numOuts++).toString();
            String value = new StringBuilder(item.getVal()).append("##").append(item.getCount().longValue()).toString();
            out.put(key, value);
            keyValueMapOutput.emit(out);
          }
        }
      }

    }

  };
  @InputPortFieldAnnotation(optional = true)
  public final transient DefaultInputPort<V> valueInput = new DefaultInputPort<V>()
  {
    @Override
    public void process(V tuple)
    {
      if (keyIndex != null) {
        String key = new StringBuilder(keyIndex.toString()).append("##").append("1").toString();
        String value = tuple.toString();

        keyValPairOutput.emit(new KeyValPair<String, String>(key, value));
      }
    }

  };
  public final transient DefaultOutputPort<Map<String, String>> keyValueMapOutput = new DefaultOutputPort<Map<String, String>>();
  public final transient DefaultOutputPort<KeyValPair<String, String>> keyValPairOutput = new DefaultOutputPort<KeyValPair<String, String>>();
}
