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
package com.datatorrent.lib.db.cache;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;

import com.datatorrent.lib.db.Connectable;
import com.datatorrent.lib.util.KeyValPair;

/**
 * Base operator that maintains a loading cache that has a maximum size and its entries expire after specified time.<br/>
 * Concrete implementations of this class should provide:<br/>
 * <ul>
 * <li>{@link Connectable} that holds connection parameters and setup/teardown functionality.</li>
 * <li>Method to extract a key from tuple.</li>
 * <li>Query to fetch the value of the key from tuple when the value is not present in the cache.</li>
 * </ul>
 *
 * @param <T> type of tuples </T>
 * @since 0.9.1
 */
public abstract class AbstractDBLookupCacheBackedOperator<T> implements Operator, Store.Backup
{
  protected final CacheProperties cacheProperties;
  protected String cacheRefreshTime;

  private transient StoreManager storeManager;

  public AbstractDBLookupCacheBackedOperator()
  {
    cacheProperties = new CacheProperties();
  }

  public final transient DefaultInputPort<T> input = new DefaultInputPort<T>()
  {
    @Override
    public void process(T tuple)
    {
      Object key = getKeyFromTuple(tuple);
      Object value = storeManager.get(key);

      if (value != null) {
        output.emit(new KeyValPair<Object, Object>(key, value));
      }
    }
  };

  public final transient DefaultOutputPort<KeyValPair<Object, Object>> output = new DefaultOutputPort<KeyValPair<Object, Object>>();

  @Override
  public void beginWindow(long l)
  {
    //Do nothing
  }

  @Override
  public void endWindow()
  {
    //Do nothing
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    storeManager = new StoreManager(new CacheStore(cacheProperties), this);
    storeManager.initialize(cacheRefreshTime);
  }

  @Override
  public void teardown()
  {
    storeManager.shutdown();
  }

  /**
   * The cache store can be refreshed every day at a specific time. This sets
   * the time. If the time is not set, cache is not refreshed.
   *
   * @param time time at which cache is refreshed everyday. Format is HH:mm:ss Z.
   */
  public void setCacheRefreshTime(String time)
  {
    cacheRefreshTime = time;
  }

  /**
   * <br>This operator receives tuples which encapsulates the keys. Concrete classes should
   * provide the implementation to extract a key from a tuple.</br>
   *
   * @param tuple input tuple to the operator.
   * @return key corresponding to the operator.
   */
  protected abstract Object getKeyFromTuple(T tuple);
}
