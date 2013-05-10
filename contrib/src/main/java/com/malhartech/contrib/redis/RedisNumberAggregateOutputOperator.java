/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.malhartech.contrib.redis;

import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang.mutable.MutableDouble;
import org.apache.commons.lang.mutable.MutableInt;
import org.apache.commons.lang.mutable.MutableLong;

/**
 *
 * @author David Yan <davidyan@malhar-inc.com>
 */
public class RedisNumberAggregateOutputOperator<K, V> extends RedisOutputOperator<K, V>
{
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

  @Override
  public void process(Map<K, V> t)
  {
    for (Map.Entry<K, V> entry: t.entrySet()) {
      K key = entry.getKey();
      V value = entry.getValue();
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
        for (Map.Entry<Object, Object> entry1: ((Map<Object, Object>)value).entrySet()) {
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
          dataMap.put(entry.getKey(), convertToNumber(value));
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

  @Override
  public void store(Map<K, Object> t)
  {
    for (Map.Entry<K, Object> entry: t.entrySet()) {
      String key = entry.getKey().toString();
      Object value = entry.getValue();
      if (value instanceof Map) {
        for (Map.Entry<Object, Object> entry1: ((Map<Object, Object>)value).entrySet()) {
          String field = entry1.getKey().toString();
          String oldVal = jedis.hget(key, field);
          if (oldVal == null) {
            jedis.hset(key, field, entry1.getValue().toString());
          }
          else {
            double increment;
            if (entry.getValue() instanceof Number) {
              increment = ((Number)entry1.getValue()).doubleValue();
            }
            else {
              increment = Double.parseDouble(entry1.getValue().toString());
            }
            jedis.hset(key, field, String.valueOf(Double.valueOf(oldVal) + increment));
          }

          // need to wait for jedis-2.2 to come out to enable the following code:
            /*
           if (entry.getValue() instanceof Number) {
           String field = entry.getKey().toString();
           // need to wait for jedis-2.2 to come out
           //jedis.hincrByFloat(key, field, ((Number)entry.getValue()).doubleValue());
           String oldVal = jedis.hget(key, field);
           if (oldVal == null) {
           jedis.hset(key, field, entry.getValue().toString());
           } else {
           String newVal = String.valueOf(Double.valueOf(oldVal) + ((Number)entry.getValue()).doubleValue());
           jedis.hset(key, field, newVal);
           }
           } else {
           jedis.hincrByFloat(key, entry.getKey().toString(), Double.parseDouble(entry.getValue().toString()));
           }
           */
        }
      }
      else {
        String oldVal = jedis.get(key);
        if (oldVal == null) {
          jedis.set(key, value.toString());
        }
        else {
          double increment;
          if (value instanceof Number) {
            increment = ((Number)value).doubleValue();
          }
          else {
            increment = Double.parseDouble(value.toString());
          }
          jedis.set(key, String.valueOf(Double.valueOf(oldVal) + increment));
        }
        /* need to wait for jedis-2.2 to come out to enable the following code:
         if (value instanceof Number) {
         jedis.incrByFloat(key, ((Number)value).doubleValue());
         } else {
         jedis.incrByFloat(key, Double.parseDouble(value.toString()));
         }
         */
      }
    }
  }

}
