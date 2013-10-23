/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>RedisOutputOperator class.</p>
 *
 * @since 0.3.2
 */
@ShipContainingJars(classes = {RedisClient.class})
public class RedisOutputOperator<K, V> extends AbstractKeyValueStoreOutputOperator<K, V>
{
  private static final Logger LOG = LoggerFactory.getLogger(RedisOutputOperator.class);
  protected transient RedisClient redisClient;
  protected transient RedisConnection<String, String> redisConnection;
  private String host = "localhost";
  private int port = 6379;
  private int dbIndex = 0;
  private int timeout= 10000;

  public void setHost(String host)
  {
    this.host = host;
  }

  public void setPort(int port)
  {
    this.port = port;
  }

  public void setDatabase(int index)
  {
    this.dbIndex = index;
  }

  public void setTimeout(int timeout)
  {
    this.timeout = timeout;
  }

  @Override
  public void setup(OperatorContext context)
  {
    redisClient = new RedisClient(host, port);
    redisConnection = redisClient.connect();
    redisConnection.select(dbIndex);
    redisConnection.setTimeout(timeout, TimeUnit.MILLISECONDS);
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
    redisConnection.multi();
  }

  @Override
  public void commitTransaction()
  {
    redisConnection.exec();
  }

  @Override
  public void rollbackTransaction()
  {
    redisConnection.discard();
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
