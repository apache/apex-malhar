/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
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
package com.datatorrent.apps.etl;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.apps.etl.MapAggregator.MapAggregateEvent;
import com.datatorrent.lib.db.DataStoreWriter;
import com.datatorrent.lib.db.cache.CacheProperties;
import com.datatorrent.lib.db.cache.CacheStore;
import com.datatorrent.lib.db.cache.StoreManager;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import javax.annotation.Nonnull;

/**
 *
 * Aggregations operator, performs the following functions.
 * 1. Aggregates the incoming tuple over its dimension combination and emits the aggregated tuple
 * 2. Persists the the dimension to the supplied store
 *
 * @param <T> type of tuple
 * @param <S> an implementation of {@link DataStoreWriter}
 */
public abstract class AggregationsOperatorBase<T, S extends DataStoreWriter<T>> extends BaseOperator
{
  protected CacheProperties cacheProps;
  protected CacheStore cache;
  @Nonnull
  protected S store;
  protected transient HashSet<T> storeEventsCache;
  protected long lastUpdatedWindowId = -1;
  protected transient long currentWindowId = -1;
  public final transient DefaultOutputPort<T> output = new DefaultOutputPort<T>();
  public final transient DefaultInputPort<T> input = new DefaultInputPort<T>()
  {
    @Override
    public void process(T event)
    {
      if (lastUpdatedWindowId < currentWindowId) {
        processTuple(event);
      }
    }
  };

  @Override
  public void setup(OperatorContext context)
  {
    cacheProps = new CacheProperties();
    cacheProps.setCacheCleanupInMillis(10000);
    cache = new CacheStore(cacheProps);
    storeEventsCache = new HashSet<T>();
    try {
      store.connect();
      lastUpdatedWindowId = store.retreiveLastUpdatedWindowId();
    }
    catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  protected abstract void processTuple(T event);

  @Override
  public void beginWindow(long windowId)
  {
    currentWindowId = windowId;
  }

  @Override
  public void endWindow()
  {
    if (lastUpdatedWindowId < currentWindowId)
    {
      store.processBulk(storeEventsCache, currentWindowId);
      lastUpdatedWindowId = currentWindowId;
    }
  }

  protected T retreiveFromStore(T event)
  {
    return store.retreive(event);
  }

  public void setStore(S store)
  {
    this.store = store;
  }

  public S getStore()
  {
    return store;
  }

}
