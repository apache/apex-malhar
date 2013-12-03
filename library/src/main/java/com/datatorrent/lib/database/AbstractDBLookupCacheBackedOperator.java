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
package com.datatorrent.lib.database;

import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;

import com.datatorrent.api.*;

import com.datatorrent.lib.util.KeyValPair;

/**
 * <br>Base opertor that maintains a loading cache that has a maximum size and its entries expire after specified time.</br>
 * <br>Concrete implementations of this class should provide:</br>
 * <ul>
 * <li>Datbase Connector: this holds connection parameters and setup/teardown functionality.</li>
 * <li>Method to extract a key from tuple.</li>
 * <li>Query to fetch the value of the key from tuple when the value is not present in the cache.</li>
 * </ul>
 *
 * @param <T> type of tuples </T>
 * @since 0.9.1
 */
public abstract class AbstractDBLookupCacheBackedOperator<T> implements Operator, ActivationListener<Context.OperatorContext>
{
  private transient StoreManager storeManager;

  protected final CacheProperties cacheProperties;

  protected String cacheRefreshTime;

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
    storeManager = new StoreManager(new CacheStore(cacheProperties), new DatabaseStore());
    storeManager.initialize(cacheRefreshTime);
  }

  @Override
  public void teardown()
  {
    storeManager.shutdown();
  }

  @Override
  public void activate(Context.OperatorContext context)
  {
    getDbConnector().setupDbConnection();
  }

  @Override
  public void deactivate()
  {
    getDbConnector().teardownDbConnection();
  }

  /**
   * Sets the maximum cache size.
   *
   * @param maxCacheSize the max size of cache in memory.
   */
  public void setMaxCacheSize(long maxCacheSize)
  {
    cacheProperties.setMaxCacheSize(maxCacheSize);
  }

  /**
   * Sets {@link CacheStore.ExpiryType} strategy.
   *
   * @param expiryType the cache entry expiry strategy.
   */
  public void setEntryExpiryStrategy(CacheStore.ExpiryType expiryType)
  {
    cacheProperties.setEntryExpiryStrategy(expiryType);
  }

  /**
   * Sets the entry expiry duration.
   *
   * @param durationInMillis the duration after which a cache entry is expired.
   */
  public void setEntryExpiryDurationInMillis(int durationInMillis)
  {
    cacheProperties.setEntryExpiryDurationInMillis(durationInMillis);
  }

  /**
   * Sets the duration at which cache is cleaned up regularly of expired entries.
   *
   * @param durationInMillis the duration after which cache is cleaned up regularly.
   */
  public void setCacheCleanupInMillis(int durationInMillis)
  {
    cacheProperties.setCacheCleanupInMillis(durationInMillis);
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

  /**
   * <br>The operator uses a database as its {@link Store.Backup}.</br>
   * <br>Sub-classes of this operator work with a specific database so they should provide a way to fetch
   * the value of a key from the database when it is missing in the {@link Store.Primary}. </br>
   *
   * @param key the key.
   * @return value of the key in the database.
   */
  protected abstract Object fetchValueFromDatabase(Object key);

  /**
   * <br>Fetch values of multiple keys from the database.</br>
   *
   * @param keys set of keys.
   * @return map of keys to their values in the database.
   */
  protected abstract Map<Object, Object> fetchValuesFromDatabase(Set<Object> keys);

  /**
   * <br>{@link StoreManager} can initialize the {@link Store.Primary} with key-value pairs.</br>
   * <br>This provides the initial data to {@link StoreManager}.</br>
   *
   * @return map of keys to their values in the database.
   */
  protected abstract Map<Object, Object> fetchStartupDataFromDatabase();

  /**
   * <br>The sub-classes of this operator interact with specific databases. This method is used to
   * access the specific {@link DBConnector} that the sub-classes use.</br>
   * @return
   */
  @Nonnull
  public abstract DBConnector getDbConnector();

  public class DatabaseStore implements Store.Backup
  {

    @Override
    public Map<Object, Object> fetchStartupData()
    {
      return fetchStartupDataFromDatabase();
    }

    @Override
    public Object getValueFor(Object key)
    {
      return fetchValueFromDatabase(key);
    }

    @Override
    public Map<Object, Object> bulkGet(Set<Object> keys)
    {
      return fetchValuesFromDatabase(keys);
    }

    @Override
    public void shutdownStore()
    {
    }
  }
}
