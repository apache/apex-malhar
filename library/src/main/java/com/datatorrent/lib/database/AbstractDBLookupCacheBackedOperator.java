package com.datatorrent.lib.database;

import java.util.concurrent.ExecutionException;

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
public abstract class AbstractDBLookupCacheBackedOperator<T> implements Operator, CacheManager.CacheUser,
  ActivationListener<Context.OperatorContext>
{
  private transient CacheManager cacheManager;

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
      Object value = null;
      try {
        value = cacheManager.getCache().get(key);
      }
      catch (ExecutionException e) {
        new RuntimeException("retrieving value", e);
      }
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
    cacheManager = new CacheManager(this, cacheProperties);
  }

  @Override
  public void teardown()
  {
    cacheManager.shutdownCleanupScheduler();
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
  public void setEntryExpiryStrategy(CacheManager.ExpiryType expiryType)
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

  @Nonnull
  public abstract DBConnector getDbConnector();
}
