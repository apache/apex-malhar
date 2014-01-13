/*
 *  Copyright (c) 2012-2014 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.contrib.redis;

import com.datatorrent.api.Partitionable;
import com.datatorrent.api.Partitionable.Partition;
import com.datatorrent.api.annotation.ShipContainingJars;
import com.datatorrent.lib.db.AggregateMapTransactionableStoreOutputOperator;
import java.util.Collection;
import redis.clients.jedis.Jedis;

/**
 *
 * @since 0.9.3
 */
@ShipContainingJars(classes = {Jedis.class})
public class RedisAggregateMapOutputOperator<K, V>
        extends AggregateMapTransactionableStoreOutputOperator<K, V, RedisStore>
        implements Partitionable<RedisAggregateMapOutputOperator<K, V>>
{
  @Override
  public Collection<Partition<RedisAggregateMapOutputOperator<K, V>>> definePartitions(Collection<Partition<RedisAggregateMapOutputOperator<K, V>>> partitions, int incrementalCapacity)
  {
    return store.definePartitionsOutputOperator(partitions, incrementalCapacity);
  }

}
