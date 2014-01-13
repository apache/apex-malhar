/*
 *  Copyright (c) 2012-2014 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.contrib.redis;

import com.datatorrent.api.Partitionable;
import com.datatorrent.api.Partitionable.Partition;
import com.datatorrent.lib.db.AbstractAggregateTransactionableStoreOutputOperator;
import com.datatorrent.lib.db.TransactionableStore;
import com.datatorrent.lib.util.KeyValPair;
import java.util.Collection;
import java.util.Map;

/**
 *
 * @since 0.9.3
 */
public class RedisAggregateKeyValPairOutputOperator<K, V> extends AbstractAggregateTransactionableStoreOutputOperator<KeyValPair<K, V>> implements Partitionable<RedisAggregateKeyValPairOutputOperator<K,V>>
{
  protected transient RedisStore redisStore;
  protected Map<Object, Object> dataMap;

  @Override
  public void setStore(TransactionableStore store)
  {
    if (store instanceof RedisStore) {
      throw new RuntimeException("Needs to be a RedisStore");
    }
    super.setStore(store);
    redisStore = (RedisStore)store;
  }

  @Override
  public void storeAggregate()
  {
    redisStore.putAll(dataMap);
  }

  @Override
  protected long getCommittedWindowId(String appId, int operatorId)
  {
    Object value = redisStore.get(getCommittedWindowKey(appId, operatorId));
    return (value == null) ? -1 : Long.valueOf(value.toString());
  }

  @Override
  protected void storeCommittedWindowId(String appId, int operatorId, long windowId)
  {
    redisStore.put(getCommittedWindowKey(appId, operatorId), windowId);
  }

  protected Object getCommittedWindowKey(String appId, int operatorId)
  {
    return "_dt_wid:" + appId + ":" + operatorId;
  }

  @Override
  public void processTuple(KeyValPair<K, V> tuple)
  {
    dataMap.put(tuple.getKey(), tuple.getValue());
  }

  @Override
  public Collection<Partition<RedisAggregateKeyValPairOutputOperator<K,V>>> definePartitions(Collection<Partition<RedisAggregateKeyValPairOutputOperator<K,V>>> partitions, int incrementalCapacity)
  {
    return redisStore.definePartitionsOutputOperator(partitions, incrementalCapacity);
  }
}
