package com.datatorrent.lib.database;

import java.util.Map;

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
    storeManager.initialize();
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
   * @param maxCacheSize the max size of cache in memory.
   */
  public void setMaxCacheSize(long maxCacheSize)
  {
    cacheProperties.setMaxCacheSize(maxCacheSize);
  }

  /**
   * @param expiryType the cache entry expiry strategy.
   */
  public void setEntryExpiryStrategy(CacheStore.ExpiryType expiryType)
  {
    cacheProperties.setEntryExpiryStrategy(expiryType);
  }

  /**
   * @param durationInMillis the duration after which a cache entry is expired.
   */
  public void setEntryExpiryDurationInMillis(int durationInMillis)
  {
    cacheProperties.setEntryExpiryDurationInMillis(durationInMillis);
  }

  /**
   * @param durationInMillis the duration after which cache is cleaned up regularly.
   */
  public void setCacheCleanupInMillis(int durationInMillis)
  {
    cacheProperties.setCacheCleanupInMillis(durationInMillis);
  }

  public abstract Object getKeyFromTuple(T tuple);

  public abstract Object fetchValueFromDatabase(Object key);

  public abstract Map<Object, Object> fetchStartupDataFromDatabase();

  @Nonnull
  public abstract DBConnector getDbConnector();

  public class DatabaseStore extends Store.Backup
  {

    @Override
    Map<Object, Object> fetchStartupData()
    {
      return fetchStartupDataFromDatabase();
    }

    @Override
    protected Object getValueFor(Object key)
    {
      return fetchValueFromDatabase(key);
    }

    @Override
    protected void shutdownStore()
    {
    }
  }
}
