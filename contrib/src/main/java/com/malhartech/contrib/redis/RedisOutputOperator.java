/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.malhartech.contrib.redis;

import com.malhartech.annotation.ShipContainingJars;
import com.malhartech.api.Context.OperatorContext;
import com.malhartech.lib.io.AbstractKeyValueStoreOutputOperator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;

/**
 *
 * @author David Yan <davidyan@malhar-inc.com>
 */
@ShipContainingJars(classes = {Jedis.class})
public class RedisOutputOperator<K, V> extends AbstractKeyValueStoreOutputOperator<K, V>
{
  protected transient Jedis jedis;
  protected transient Transaction currentTransaction;
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
    super.setup(context);
    jedis = new Jedis(host, port, timeout);
  }

  @Override
  public String get(String key)
  {
    if (currentTransaction == null) {
      return jedis.get(key);
    }
    else {
      return currentTransaction.get(key).get();
    }
  }

  @Override
  public void put(String key, String value)
  {
    if (currentTransaction == null) {
      jedis.set(key, value);
    }
    else {
      currentTransaction.set(key, value);
    }
  }

  @Override
  public void startTransaction()
  {
    if (currentTransaction != null) {
      currentTransaction.discard();
    }
    currentTransaction = jedis.multi();
  }

  @Override
  public void commitTransaction()
  {
    currentTransaction.exec();
    currentTransaction = null;
  }

  @Override
  public void store(Map<K, Object> t)
  {
    for (Map.Entry<K, Object> entry: t.entrySet()) {
      Object value = entry.getValue();
      if (value instanceof Map) {
        for (Map.Entry<Object, Object> entry1: ((Map<Object, Object>)value).entrySet()) {
          jedis.hset(entry.getKey().toString(), entry1.getKey().toString(), entry1.getValue().toString());
        }
      }
      else if (value instanceof Set) {
        for (Object o: (Set)value) {
          jedis.sadd(entry.getKey().toString(), o.toString());
        }
      }
      else if (value instanceof List) {
        int i = 0;
        for (Object o: (List)value) {
          jedis.lset(entry.getKey().toString(), i++, o.toString());
        }
      }
      else {
        jedis.set(entry.getKey().toString(), value.toString());
      }
    }
  }

}
