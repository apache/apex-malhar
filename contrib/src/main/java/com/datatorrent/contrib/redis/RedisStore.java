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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;

import com.datatorrent.lib.db.TransactionableKeyValueStore;

/**
 * Provides the implementation of a Redis store.
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
  protected int keyExpiryTime = -1;
  private transient Transaction transaction;

  /**
   * Gets the host.
   *
   * @return
   */
  public String getHost()
  {
    return host;
  }

  /**
   * Sets the host.
   *
   * @param host
   */
  public void setHost(String host)
  {
    this.host = host;
  }

  /**
   * Gets the port.
   *
   * @return
   */
  public int getPort()
  {
    return port;
  }

  /**
   * Sets the port.
   *
   * @param port
   */
  public void setPort(int port)
  {
    this.port = port;
  }

  /**
   * Gets the DB index.
   *
   * @return
   */
  public int getDbIndex()
  {
    return dbIndex;
  }

  /**
   * Sets the DB index.
   *
   * @param dbIndex
   */
  public void setDbIndex(int dbIndex)
  {
    this.dbIndex = dbIndex;
  }

  /**
   * Gets the key expiry time.
   *
   * @return
   */
  public int getKeyExpiryTime()
  {
    return keyExpiryTime;
  }

  /**
   * Sets the key expiry time.
   *
   * @param keyExpiryTime
   */
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
    jedis.disconnect();
  }

  @Override
  public boolean connected()
  {
    return jedis != null && jedis.isConnected();
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
    if (isInTransaction()) {
      throw new RuntimeException("Cannot call get when in redis transaction");
    }
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
    if (isInTransaction()) {
      throw new RuntimeException("Cannot call get when in redis transaction");
    }
    return (List<Object>) (List<?>) jedis.mget(keys.toArray(new String[]{}));
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  @Override
  public void put(Object key, Object value)
  {
    if (isInTransaction()) {
      if (value instanceof Map) {
        transaction.hmset(key.toString(), (Map) value);
      }
      else {
        transaction.set(key.toString(), value.toString());
      }
      if (keyExpiryTime != -1) {
        transaction.expire(key.toString(), keyExpiryTime);
      }
    }
    else {
      if (value instanceof Map) {
        jedis.hmset(key.toString(), (Map) value);
      }
      else {
        jedis.set(key.toString(), value.toString());
      }
      if (keyExpiryTime != -1) {
        jedis.expire(key.toString(), keyExpiryTime);
      }
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
    if (isInTransaction()) {
      transaction.mset(params.toArray(new String[]{}));
    }
    else {
      jedis.mset(params.toArray(new String[]{}));
    }
  }

  @Override
  public void remove(Object key)
  {
    if (isInTransaction()) {
      transaction.del(key.toString());
    }
    else {
      jedis.del(key.toString());
    }
  }

  /**
   * Calls hincrbyfloat on the redis store.
   *
   * @param key
   * @param field
   * @param doubleValue
   */
  public void hincrByFloat(String key, String field, double doubleValue)
  {
    if (isInTransaction()) {
      transaction.hincrByFloat(key, field, doubleValue);
      if (keyExpiryTime != -1) {
        transaction.expire(key, keyExpiryTime);
      }
    }
    else {
      jedis.hincrByFloat(key, field, doubleValue);
      if (keyExpiryTime != -1) {
        jedis.expire(key, keyExpiryTime);
      }
    }
  }

  /**
   * Calls incrbyfloat on the redis store.
   *
   * @param key
   * @param doubleValue
   */
  public void incrByFloat(String key, double doubleValue)
  {
    if (isInTransaction()) {
      transaction.incrByFloat(key, doubleValue);
      if (keyExpiryTime != -1) {
        transaction.expire(key, keyExpiryTime);
      }
    }
    else {
      jedis.incrByFloat(key, doubleValue);
      if (keyExpiryTime != -1) {
        jedis.expire(key, keyExpiryTime);
      }
    }
  }

  @Override
  public long getCommittedWindowId(String appId, int operatorId)
  {
    Object value = get(getCommittedWindowKey(appId, operatorId));
    return (value == null) ? -1 : Long.valueOf(value.toString());
  }

  @Override
  public void storeCommittedWindowId(String appId, int operatorId, long windowId)
  {
    put(getCommittedWindowKey(appId, operatorId), windowId);
  }

  protected Object getCommittedWindowKey(String appId, int operatorId)
  {
    return "_dt_wid:" + appId + ":" + operatorId;
  }

  @Override
  public void removeCommittedWindowId(String appId, int operatorId)
  {
    remove(getCommittedWindowKey(appId, operatorId));
  }

}
