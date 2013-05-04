/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.malhartech.contrib.redis;

import java.util.Map;

/**
 *
 * @author David Yan <davidyan@malhar-inc.com>
 */
public class RedisNumberAggregateOutputOperator<B, K, V> extends RedisOutputOperator<B, K, V>
{
  @Override
  public void store(Map<B, Map<K, V>> t)
  {
    for (Map.Entry<B, Map<K, V>> entry1: t.entrySet()) {
      for (Map.Entry<K, V> entry2: entry1.getValue().entrySet()) {
        String key = entry1.getKey().toString() + "|" + entry2.getKey().toString();
        V value = entry2.getValue();
        if (value instanceof Map) {
          for (Map.Entry<Object, Object> entry: ((Map<Object, Object>)value).entrySet()) {
            String field = entry.getKey().toString();
            String oldVal = jedis.hget(key, field);
            if (oldVal == null) {
              jedis.hset(key, field, entry.getValue().toString());
            }
            else {
              double increment;
              if (entry.getValue() instanceof Number) {
                increment = ((Number)entry.getValue()).doubleValue();
              }
              else {
                increment = Double.parseDouble(entry.getValue().toString());
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

}
