package com.datatorrent.contrib.redis;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class RedisStringOutputOperator<K> extends RedisOutputOperator<K,Map<String,String>>
{

  @Override
  public void store(Map<K, Object> t)
  {
    for (Map.Entry<K, Object> entry : t.entrySet()) {
      Object value = entry.getValue();
      if (value instanceof Map) {        
        redisConnection.hmset(entry.getKey().toString(), (Map) value);
      } else if (value instanceof Set) {
        for (Object o : (Set) value) {
          redisConnection.sadd(entry.getKey().toString(), o.toString());
        }
      } else if (value instanceof List) {
        int i = 0;
        for (Object o : (List) value) {
          redisConnection.lset(entry.getKey().toString(), i++, o.toString());
        }
      } else {
        redisConnection.set(entry.getKey().toString(), value.toString());
      }
      if (keyExpiryTime != -1) {
        redisConnection.expire(entry.getKey().toString(), keyExpiryTime);
      }
    }
  }
}
