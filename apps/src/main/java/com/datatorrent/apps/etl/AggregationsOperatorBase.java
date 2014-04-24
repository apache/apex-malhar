/*
 *  Copyright (c) 2012-2014 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.apps.etl;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.lib.db.DataStoreWriter;
import com.datatorrent.lib.db.cache.CacheProperties;
import com.datatorrent.lib.db.cache.CacheStore;
import java.io.IOException;
import javax.annotation.Nonnull;

/**
 *
 * Aggregations operator, performs the following functions.
 * 1. Aggregates the incoming tuple over its dimension combination and emits the aggregated tuple
 * 2. Persists the the dimension to the supplied store
 * @param <T> type of tuple
 * @param <S> an implementation of {@link DataStoreWriter}
 */
public abstract class AggregationsOperatorBase<T, S extends DataStoreWriter<T>> extends BaseOperator
{
  protected CacheProperties cacheProps;
  protected CacheStore cache;
  @Nonnull
  protected S store;
  public final transient DefaultOutputPort<T> output = new DefaultOutputPort<T>();
  public final transient DefaultInputPort<T> input = new DefaultInputPort<T>()
  {
    @Override
    public void process(T event)
    {
      processTuple(event);
    }
  };

  @Override
  public void setup(OperatorContext context)
  {
    cacheProps = new CacheProperties();
    cacheProps.setCacheCleanupInMillis(10000);
    cache = new CacheStore(cacheProps);
    try {
      store.connect();
    }
    catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  protected abstract void processTuple(T event);

  protected void updateStore(T event)
  {
    store.process(event);
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
