/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.contrib.redis;

import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang.mutable.MutableDouble;
import org.apache.commons.lang.mutable.MutableLong;

/**
 * Provides routines of number aggregation. Not to be exported outside of this package
 * @since 0.9.3
 */
class NumberSummation<K, V>
{
  private AbstractRedisAggregateOutputOperator<?> operator;
  private Map<Object, Object> dataMap;

  NumberSummation(AbstractRedisAggregateOutputOperator<?> operator, Map<Object, Object> dataMap)
  {
    this.operator = operator;
    this.dataMap = dataMap;
  }

  protected Number convertToNumber(Object o)
  {
    if (o == null) {
      return null;
    }
    else if (o instanceof MutableDouble || o instanceof MutableLong) {
      return (Number)o;
    }
    else if (o instanceof Double || o instanceof Float) {
      return new MutableDouble((Number)o);
    }
    else if (o instanceof Number) {
      return new MutableLong((Number)o);
    }
    else {
      return new MutableDouble(o.toString());
    }
  }

  @SuppressWarnings("unchecked")
  public void storeAggregate()
  {
    for (Map.Entry<Object, Object> entry : dataMap.entrySet()) {
      Object key = entry.getKey();
      Object value = entry.getValue();
      if (value instanceof Map) {
        for (Map.Entry<Object, Object> entry1 : ((Map<Object, Object>)value).entrySet()) {
          String field = entry1.getKey().toString();
          Object hvalue = entry1.getValue();
          if (hvalue instanceof Number) {
            operator.getStore().hincrByFloat(key.toString(), field, ((Number)hvalue).doubleValue());
          }
          else {
            operator.getStore().hincrByFloat(key.toString(), field, Double.parseDouble(hvalue.toString()));
          }
        }
      }
      else {
        if (value instanceof Number) {
          operator.getStore().incrByFloat(key.toString(), ((Number)value).doubleValue());
        }
        else {
          operator.getStore().incrByFloat(key.toString(), Double.parseDouble(value.toString()));
        }
      }
    }
    dataMap.clear();
  }

  @SuppressWarnings("unchecked")
  public void process(K key, V value) throws RuntimeException
  {
    if (value instanceof Map) {
      Object o = dataMap.get(key);
      if (o == null) {
        o = new HashMap<Object, Object>();
        dataMap.put(key, o);
      }

      if (!(o instanceof Map)) {
        throw new RuntimeException("Values of unexpected type in data map. Expecting Map");
      }
      Map<Object, Object> map = (Map<Object, Object>)o;
      for (Map.Entry<Object, Object> entry1 : ((Map<Object, Object>)value).entrySet()) {
        Object field = entry1.getKey();
        Number oldVal = (Number)map.get(field);
        if (oldVal == null) {
          map.put(field, convertToNumber(entry1.getValue()));
        }
        else if (oldVal instanceof MutableDouble) {
          ((MutableDouble)oldVal).add(convertToNumber(entry1.getValue()));
        }
        else if (oldVal instanceof MutableLong) {
          ((MutableLong)oldVal).add(convertToNumber(entry1.getValue()));
        }
        else {
          throw new RuntimeException("Values of unexpected type in data map value field type. Expecting MutableLong or MutableDouble");
        }
      }
    }
    else {
      Number oldVal = convertToNumber(dataMap.get(key));
      if (oldVal == null) {
        dataMap.put(key, convertToNumber(value));
      }
      else {
        if (oldVal instanceof MutableDouble) {
          ((MutableDouble)oldVal).add(convertToNumber(value));
        }
        else if (oldVal instanceof MutableLong) {
          ((MutableLong)oldVal).add(convertToNumber(value));
        }
        else {
          // should not get here
          throw new RuntimeException("Values of unexpected type in data map value type. Expecting MutableLong or MutableDouble");
        }
      }
    }
  }
}
