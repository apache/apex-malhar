/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.datatorrent.contrib.redis;

import com.datatorrent.lib.io.AbstractKeyValueStoreOutputOperator;
import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisConnection;
import com.lambdaworks.redis.RedisException;
import com.datatorrent.api.annotation.ShipContainingJars;
import com.datatorrent.api.Context.OperatorContext;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 *
 * @author David Yan <davidyan@malhar-inc.com>
 */
@ShipContainingJars(classes = {RedisClient.class})
public class RedisOutputOperator<K, V> extends AbstractKeyValueStoreOutputOperator<K, V>
{
  protected transient RedisClient redisClient;
  protected transient RedisConnection<String, String> redisConnection;
  private String host = "localhost";
  private int port = 6379;
  private int dbIndex = 0;

  public void setHost(String host)
  {
    this.host = host;
  }

  public void setPort(int port)
  {
    this.port = port;
  }

  public void selectDatabase(int index)
  {
    this.dbIndex = index;
  }

  @Override
  public void setup(OperatorContext context)
  {
    redisClient = new RedisClient(host, port);
    redisConnection = redisClient.connect();
    redisConnection.select(dbIndex);
    super.setup(context);
  }

  @Override
  public String get(String key)
  {
    return redisConnection.get(key);
  }

  @Override
  public void put(String key, String value)
  {
    redisConnection.set(key, value);
  }

  @Override
  public void startTransaction()
  {
    try {
      redisConnection.discard();
    } catch (RedisException ex) {
      // ignore
    }
    redisConnection.multi();
  }

  @Override
  public void commitTransaction()
  {
    redisConnection.exec();
  }

  @Override
  public void store(Map<K, Object> t)
  {
    for (Map.Entry<K, Object> entry: t.entrySet()) {
      Object value = entry.getValue();
      if (value instanceof Map) {
        for (Map.Entry<Object, Object> entry1: ((Map<Object, Object>)value).entrySet()) {
          redisConnection.hset(entry.getKey().toString(), entry1.getKey().toString(), entry1.getValue().toString());
        }
      }
      else if (value instanceof Set) {
        for (Object o: (Set)value) {
          redisConnection.sadd(entry.getKey().toString(), o.toString());
        }
      }
      else if (value instanceof List) {
        int i = 0;
        for (Object o: (List)value) {
          redisConnection.lset(entry.getKey().toString(), i++, o.toString());
        }
      }
      else {
        redisConnection.set(entry.getKey().toString(), value.toString());
      }
    }
  }

}
