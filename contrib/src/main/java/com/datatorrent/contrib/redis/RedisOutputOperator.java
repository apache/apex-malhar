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
import com.datatorrent.api.annotation.ShipContainingJars;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultPartition;
import com.datatorrent.api.Partitionable.Partition;
import com.datatorrent.api.Partitionable.PartitionKeys;
import com.datatorrent.api.Partitionable;
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * RedisOutputOperator class.
 * </p>
 *
 * @since 0.3.2
 */
@ShipContainingJars(classes = { RedisClient.class })
@SuppressWarnings({ "rawtypes", "unchecked", "unused" })
public class RedisOutputOperator<K, V> extends AbstractKeyValueStoreOutputOperator<K, V> implements Partitionable<RedisOutputOperator<K,V>>
{
  private static final Logger LOG = LoggerFactory.getLogger(RedisOutputOperator.class);
  protected transient RedisClient redisClient;
  protected transient RedisConnection<String, String> redisConnection;
  private String host = "localhost";
  private int port = 6379;
  private int dbIndex = 0;
  private int timeout = 10000;
  protected long keyExpiryTime = -1;
  private String connectionList;

  public long getKeyExpiryTime()
  {
    return keyExpiryTime;
  }

  public void setKeyExpiryTime(long keyExpiryTime)
  {
    this.keyExpiryTime = keyExpiryTime;
  }

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
    if (connectionList != null) {
      setRedis();
    }
    redisClient = new RedisClient(host, port);
    redisConnection = redisClient.connect();
    redisConnection.select(dbIndex);
    redisConnection.setTimeout(timeout, TimeUnit.MILLISECONDS);
    super.setup(context);
  }

  private void setRedis()
  {
    String[] connectionArr = connectionList.split(",");
    if(connectionArr.length < 1){
      LOG.warn("since no value is set, setting everything to default");
      return;
    }
    String[] redisInstance = connectionArr[0].split(":");
    host = redisInstance[0];
    try {
      port = Integer.valueOf(redisInstance[1].trim());
    } catch (NumberFormatException ex) {
      LOG.error("defaulting the value to default port 6379 as there is error {} ", ex.getMessage());
      port = 6379;
    } catch(ArrayIndexOutOfBoundsException ex){
      LOG.error("defaulting the value to default port 6379 as there is error {} ", ex.getMessage());
      port = 6379;
    }
    try{
      dbIndex = Integer.valueOf(connectionArr[1].trim());
    }catch (NumberFormatException ex) {
      LOG.error("defaulting the value to default DB 0 as there is error {} ", ex.getMessage());
      dbIndex = 0;
    }catch (ArrayIndexOutOfBoundsException ex) {
      LOG.error("defaulting the value to default DB 0 as there is error {} ", ex.getMessage());
      dbIndex = 0;
    }
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
    if (keyExpiryTime != -1) {
      redisConnection.expire(key, keyExpiryTime);
    }
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
    for (Map.Entry<K, Object> entry : t.entrySet()) {
      Object value = entry.getValue();
      if (value instanceof Map) {
        /*
        redisConnection.hmset(entry.getKey().toString(), (Map) value);
        */
        
        for (Map.Entry<Object, Object> entry1 : ((Map<Object, Object>) value).entrySet()) {
          redisConnection.hset(entry.getKey().toString(), entry1.getKey().toString(), entry1.getValue().toString());
        }
        
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

  public String getConnectionList()
  {
    return connectionList;
  }

  public void setConnectionList(String connectionList)
  {
    this.connectionList = connectionList;
  }

  @Override
  public Collection<Partition<RedisOutputOperator<K,V>>> definePartitions(Collection<Partition<RedisOutputOperator<K,V>>> partitions, int incrementalCapacity)
  {
    Collection c = partitions;
    Collection<Partition<RedisOutputOperator<K, V>>> operatorPartitions = c;
    Partition<RedisOutputOperator<K, V>> template = null;
    Iterator<Partition<RedisOutputOperator<K, V>>> itr = operatorPartitions.iterator();
    template = itr.next();
    String[] connectionArr = connectionList.trim().split("\\|");
    int size = connectionArr.length;
    if (size > (incrementalCapacity + operatorPartitions.size()))
      size = incrementalCapacity + operatorPartitions.size();

    int partitionBits = (Integer.numberOfLeadingZeros(0) - Integer.numberOfLeadingZeros(size - 1));
    int partitionMask = 0;
    if (partitionBits > 0) {
      partitionMask = -1 >>> (Integer.numberOfLeadingZeros(-1)) - partitionBits;
    }
    LOG.debug("partition mask {}", partitionMask);
    ArrayList<Partition<RedisOutputOperator<K,V>>> operList = new ArrayList<Partitionable.Partition<RedisOutputOperator<K,V>>>(size);

    while (size > 0) {
      size--;
      RedisOutputOperator<K, V> opr = new RedisOutputOperator<K, V>();
      opr.setConnectionList(connectionArr[size].trim());
      opr.setKeyExpiryTime(keyExpiryTime);
      opr.setTimeout(timeout);
      opr.setContinueOnError(continueOnError);
      opr.setName(getName());
      Partition<RedisOutputOperator<K, V>> p = new DefaultPartition(opr);
      operList.add(p);
    }

    for (int i = 0; i <= partitionMask; i++) {
      Partition<RedisOutputOperator<K, V>> p = operList.get(i % operList.size());
      PartitionKeys pks = p.getPartitionKeys().get(input);
      if (pks == null) {
        p.getPartitionKeys().put(input, new PartitionKeys(partitionMask, Sets.newHashSet(i)));
      } else {
        pks.partitions.add(i);
      }
      pks = p.getPartitionKeys().get(inputInd);
      if (pks == null) {
        p.getPartitionKeys().put(inputInd, new PartitionKeys(partitionMask, Sets.newHashSet(i)));
      } else {
        pks.partitions.add(i);
      }
    }

    return operList;
  }

}
