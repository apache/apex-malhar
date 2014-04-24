package com.datatorrent.contrib.mongodb;

import com.datatorrent.api.Context;

import com.datatorrent.lib.db.cache.AbstractDBLookupCacheBackedOperator;

/**
 * <br>This is {@link AbstractDBLookupCacheBackedOperator} which retrieves value of a key
 * from MongoDB</br>
 *
 * @param <T> type of input tuples </T>
 * @since 0.9.1
 */
public abstract class MongoDBLookupCacheBackedOperator<T> extends AbstractDBLookupCacheBackedOperator<T>
{
  private final MongoDBConnectable store;

  public MongoDBLookupCacheBackedOperator()
  {
    super();
    store = new MongoDBConnectable();
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    store.connect();
    super.setup(context);
  }

  @Override
  public void teardown()
  {
    store.disconnect();
    super.teardown();
  }
}
