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

import com.datatorrent.lib.db.TransactionableKeyValueStore;
import java.io.IOException;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;

/**
 *
 * @since 0.9.3
 */
public class RedisStore implements TransactionableKeyValueStore
{
  private static final Logger LOG = LoggerFactory.getLogger(RedisStore.class);
  protected transient Jedis jedis;
  private String host = "localhost";
  private int port = 6379;
  private int dbIndex = 0;
  private int timeout = 10000;
  protected int keyExpiryTime = -1;
  private transient Transaction transaction;

  public String getHost()
  {
    return host;
  }

  public void setHost(String host)
  {
    this.host = host;
  }

  public int getPort()
  {
    return port;
  }

  public void setPort(int port)
  {
    this.port = port;
  }

  public int getDbIndex()
  {
    return dbIndex;
  }

  public void setDbIndex(int dbIndex)
  {
    this.dbIndex = dbIndex;
  }

  public int getTimeout()
  {
    return timeout;
  }

  public void setTimeout(int timeout)
  {
    this.timeout = timeout;
  }

  public int getKeyExpiryTime()
  {
    return keyExpiryTime;
  }

  public void setKeyExpiryTime(int keyExpiryTime)
  {
    this.keyExpiryTime = keyExpiryTime;
  }

  @Override
  public void connect() throws IOException
  {
    jedis = new Jedis(host, port);
    jedis.connect();
    jedis.select(dbIndex);
  }

  @Override
  public void disconnect() throws IOException
  {
    jedis.shutdown();
  }

  @Override
  public boolean isConnected()
  {
    return jedis.isConnected();
  }

  @Override
  public void beginTransaction()
  {
    transaction = jedis.multi();
  }

  @Override
  public void commitTransaction()
  {
    transaction.exec();
    transaction = null;
  }

  @Override
  public void rollbackTransaction()
  {
    transaction.discard();
    transaction = null;
  }

  @Override
  public boolean isInTransaction()
  {
    return transaction != null;
  }

  /**
   * Gets the value given the key.
   * Note that it does NOT work with hash values or list values
   *
   * @param key
   * @return
   */
  @Override
  public Object get(Object key)
  {
    return jedis.get(key.toString());
  }

  /**
   * Gets all the values given the keys.
   * Note that it does NOT work with hash values or list values
   *
   * @param keys
   * @return
   */
  @SuppressWarnings("unchecked")
  @Override
  public List<Object> getAll(List<Object> keys)
  {
    return (List<Object>)(List<?>)jedis.mget(keys.toArray(new String[] {}));
  }

  @SuppressWarnings("unchecked")
  @Override
  public void put(Object key, Object value)
  {
    if (value instanceof Map) {
      jedis.hmset(host, (Map)value);
    }
    else {
      jedis.set(key.toString(), value.toString());
    }
    if (keyExpiryTime != -1) {
      jedis.expire(key.toString(), keyExpiryTime);
    }
  }

  @Override
  public void putAll(Map<Object, Object> m)
  {
    List<String> params = new ArrayList<String>();
    for (Map.Entry<Object, Object> entry : m.entrySet()) {
      params.add(entry.getKey().toString());
      params.add(entry.getValue().toString());
    }
    jedis.mset(params.toArray(new String[] {}));
  }

  @Override
  public void remove(Object key)
  {
    jedis.del(key.toString());
  }

  public void hincrByFloat(String key, String field, double doubleValue)
  {
    jedis.hincrByFloat(key, field, doubleValue);
    if (keyExpiryTime != -1) {
      jedis.expire(key, keyExpiryTime);
    }
  }

  public void incrByFloat(String key, double doubleValue)
  {
    jedis.incrByFloat(key, doubleValue);
    if (keyExpiryTime != -1) {
      jedis.expire(key, keyExpiryTime);
    }
  }

}
