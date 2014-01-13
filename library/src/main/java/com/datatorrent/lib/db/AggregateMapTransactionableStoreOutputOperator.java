/*
 *  Copyright (c) 2012-2014 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.lib.db;

import com.datatorrent.api.Partitionable;
import com.datatorrent.api.Partitionable.Partition;
import com.datatorrent.lib.db.AbstractAggregateTransactionableStoreOutputOperator;
import com.datatorrent.lib.db.TransactionableStore;
import java.util.Collection;
import java.util.Map;

/**
 *
 * @since 0.9.3
 */
public class AggregateMapTransactionableStoreOutputOperator<K, V, S extends TransactionableKeyValueStore> extends AbstractAggregateTransactionableStoreOutputOperator<Map<K, V>, S>
{
  protected Map<Object, Object> dataMap;

  @Override
  public void storeAggregate()
  {
    store.putAll(dataMap);
  }

  @Override
  protected long getCommittedWindowId(String appId, int operatorId)
  {
    Object value = store.get(getCommittedWindowKey(appId, operatorId));
    return (value == null) ? -1 : Long.valueOf(value.toString());
  }

  @Override
  protected void storeCommittedWindowId(String appId, int operatorId, long windowId)
  {
    store.put(getCommittedWindowKey(appId, operatorId), windowId);
  }

  protected Object getCommittedWindowKey(String appId, int operatorId)
  {
    return "_dt_wid:" + appId + ":" + operatorId;
  }

  @Override
  public void processTuple(Map<K, V> tuple)
  {
    dataMap.putAll(tuple);
  }
  
}
