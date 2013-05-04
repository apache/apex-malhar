/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.malhartech.contrib.redis;

import com.malhartech.api.Context.OperatorContext;
import com.malhartech.lib.io.AbstractBucketKeyValueStoreOutputOperator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import redis.clients.jedis.Jedis;

/**
 *
 * @author David Yan <davidyan@malhar-inc.com>
 */
public class RedisOutputOperator<B, K, V> extends AbstractBucketKeyValueStoreOutputOperator<B, K, V>
{
  protected Jedis jedis;
  private String host = "localhost";
  private int port = 6379;
  private int timeout = 1000;

  public void setHost(String host)
  {
    this.host = host;
  }

  public void setPort(int port)
  {
    this.port = port;
  }

  public void setTimeout(int timeout)
  {
    this.timeout = timeout;
  }

  @Override
  public void setup(OperatorContext context)
  {
    jedis = new Jedis(host, port, timeout);
  }

  @Override
  public void store(Map<B, Map<K, V>> t)
  {
    for (Map.Entry<B, Map<K, V>> entry1: t.entrySet()) {
      for (Map.Entry<K, V> entry2: entry1.getValue().entrySet()) {
        String key = entry1.getKey().toString() + "|" + entry2.getKey().toString();
        V value = entry2.getValue();
        if (value instanceof Map) {
          for (Map.Entry<Object, Object> entry: ((Map<Object, Object>)value).entrySet()) {
            jedis.hset(key, entry.getKey().toString(), entry.getValue().toString());
          }
        }
        else if (value instanceof Set) {
          for (Object o: (Set<Object>)value) {
            jedis.sadd(key, o.toString());
          }
        }
        else if (value instanceof List) {
          int i = 0;
          for (Object o: (List<Object>)value) {
            jedis.lset(key, i++, o.toString());
          }
        }
        else {
          jedis.set(key, value.toString());
        }
      }
    }
  }

}
