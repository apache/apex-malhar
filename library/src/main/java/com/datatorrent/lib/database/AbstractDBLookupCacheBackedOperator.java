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

  public abstract Object getKeyFromTuple(T tuple);

  public abstract Object fetchValueFromDatabase(Object key);

  public abstract Map<Object, Object> fetchValuesFromDatabase(Set<Object> keys);

  public abstract Map<Object, Object> fetchStartupDataFromDatabase();

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
