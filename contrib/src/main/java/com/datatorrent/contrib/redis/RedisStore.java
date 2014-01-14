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

import com.datatorrent.api.DefaultPartition;
import com.datatorrent.api.Partitionable;
import com.datatorrent.api.Partitionable.Partition;
import com.datatorrent.api.Partitionable.PartitionKeys;
import com.datatorrent.lib.db.AbstractTransactionableStoreOutputOperator;
import com.datatorrent.lib.db.TransactionableKeyValueStore;
import com.google.common.collect.Sets;
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
  private String connectionList;
  private Transaction transaction;

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

  public String getConnectionList()
  {
    return connectionList;
  }

  public void setConnectionList(String connectionList)
  {
    this.connectionList = connectionList;
  }

  private void setRedis()
  {
    String[] connectionArr = connectionList.split(",");
    if (connectionArr.length < 1) {
      LOG.warn("since no value is set, setting everything to default");
      return;
    }
    String[] redisInstance = connectionArr[0].split(":");
    host = redisInstance[0];
    try {
      port = Integer.valueOf(redisInstance[1].trim());
    }
    catch (NumberFormatException ex) {
      LOG.error("defaulting the value to default port 6379 as there is error {} ", ex.getMessage());
      port = 6379;
    }
    catch (ArrayIndexOutOfBoundsException ex) {
      LOG.error("defaulting the value to default port 6379 as there is error {} ", ex.getMessage());
      port = 6379;
    }
    try {
      dbIndex = Integer.valueOf(connectionArr[1].trim());
    }
    catch (NumberFormatException ex) {
      LOG.error("defaulting the value to default DB 0 as there is error {} ", ex.getMessage());
      dbIndex = 0;
    }
    catch (ArrayIndexOutOfBoundsException ex) {
      LOG.error("defaulting the value to default DB 0 as there is error {} ", ex.getMessage());
      dbIndex = 0;
    }
  }

  @Override
  public void connect() throws IOException
  {
    if (connectionList != null) {
      setRedis();
    }
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
  @Override
  public List<Object> getAll(List<Object> keys)
  {
    return (List<Object>)(List<?>)jedis.mget(keys.toArray(new String[] {}));
  }

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

  public <T extends AbstractTransactionableStoreOutputOperator> Collection<Partition<T>> definePartitionsOutputOperator(Collection<Partition<T>> partitions, int incrementalCapacity)
  {
    Collection<Partition<T>> operatorPartitions = partitions;
    T partitionedInstance = partitions.iterator().next().getPartitionedInstance();
    if (connectionList == null) {
      connectionList = host + ":" + port + "," + dbIndex;
    }
    String[] connectionArr = connectionList.trim().split("\\|");
    int size = connectionArr.length;
    if (size > (incrementalCapacity + operatorPartitions.size())) {
      size = incrementalCapacity + operatorPartitions.size();
    }

    int partitionBits = (Integer.numberOfLeadingZeros(0) - Integer.numberOfLeadingZeros(size - 1));
    int partitionMask = 0;
    if (partitionBits > 0) {
      partitionMask = -1 >>> (Integer.numberOfLeadingZeros(-1)) - partitionBits;
    }
    LOG.debug("partition mask {}", partitionMask);
    ArrayList<Partition<T>> operList = new ArrayList<Partitionable.Partition<T>>(size);

    while (size > 0) {
      size--;
      try {
        AbstractTransactionableStoreOutputOperator newOperator = partitionedInstance.getClass().newInstance();
        newOperator.setStore(this);
        Partition<T> p = new DefaultPartition(newOperator);
        operList.add(p);
      }
      catch (Exception ex) {
        throw new RuntimeException(ex);
      }
    }

    for (int i = 0; i <= partitionMask; i++) {
      Partition<T> p = operList.get(i % operList.size());
      PartitionKeys pks = p.getPartitionKeys().get(partitionedInstance.input);
      if (pks == null) {
        p.getPartitionKeys().put(partitionedInstance.input, new PartitionKeys(partitionMask, Sets.newHashSet(i)));
      }
      else {
        pks.partitions.add(i);
      }
    }
    return operList;
  }

}
