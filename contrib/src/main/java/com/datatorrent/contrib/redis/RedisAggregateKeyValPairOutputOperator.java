/*
 *  Copyright (c) 2012-2014 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.contrib.redis;

import com.datatorrent.api.Partitionable;
import com.datatorrent.api.Partitionable.Partition;
import com.datatorrent.api.annotation.ShipContainingJars;
import com.datatorrent.lib.db.AggregateKeyValPairTransactionableStoreOutputOperator;
import java.util.Collection;
import redis.clients.jedis.Jedis;

/**
 *
 * @since 0.9.3
 */
@ShipContainingJars(classes = {Jedis.class})
public class RedisAggregateKeyValPairOutputOperator<K, V>
        extends AggregateKeyValPairTransactionableStoreOutputOperator<K, V, RedisStore>
        implements Partitionable<RedisAggregateKeyValPairOutputOperator<K, V>>
{
  @Override
  public Collection<Partition<RedisAggregateKeyValPairOutputOperator<K, V>>> definePartitions(Collection<Partition<RedisAggregateKeyValPairOutputOperator<K, V>>> partitions, int incrementalCapacity)
  {
    return store.definePartitionsOutputOperator(partitions, incrementalCapacity);
  }

}
