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
package org.apache.apex.malhar.lib.converter;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;

/**
 *
 * This operator converts Map<K, String> to Map<K, Number> for numeric string values
 *
 * @displayName String to Number value converter for Map
 * @category Tuple Converters
 * @tags string
 *
 * @since 3.0.0
 */
public class StringValueToNumberConverterForMap<K> extends BaseOperator
{

  /**
   * Input port which accepts Map<K, Numeric String>.
   */
  public final transient DefaultInputPort<Map<K, String>> input = new DefaultInputPort<Map<K, String>>()
  {
    @Override
    public void process(Map<K, String> tuple)
    {
      Map<K, Number> outputMap = new HashMap<K, Number>();
      for (Entry<K, String> entry : tuple.entrySet()) {
        String val = entry.getValue();
        if (val == null) {
          return;
        }
        double tvalue = 0;
        boolean errortuple = false;
        try {
          tvalue = Double.parseDouble(val);
        } catch (NumberFormatException e) {
          errortuple = true;
        }
        if (!errortuple) {
          outputMap.put(entry.getKey(), tvalue);
        }
      }
      output.emit(outputMap);
    }

  };

  /*
   * Output port which outputs Map<K, Number> after converting numeric string to number
   */
  public final transient DefaultOutputPort<Map<K, Number>> output = new DefaultOutputPort<Map<K, Number>>();
}
