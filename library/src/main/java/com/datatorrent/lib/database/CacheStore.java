package com.datatorrent.lib.database;

import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

/**
 * <br>A {@link Store.Primary} that keeps key/value pairs in memory.</br>
 * <br></br>
 * <br>The cache storage is defined by:</br>
 * <ul>
 * <li>Transient: It is not checkpointed.</li>
 * <li>Max Cache Size: it starts evicting entries before this limit is exceeded.</li>
 * <li>Entry expirty time: the entries epire after the specified duration.</li>
 * <li>Cache cleanup interval: the interval at which the cache is cleaned up of expired entries periodically.</li>
 * </ul>
 * <br>These properties of the cache are encapsulated in {@link CacheProperties}</br>
 */
public class CacheStore extends Store.Primary
{
  private transient ScheduledExecutorService cleanupScheduler;
  private transient Cache<Object, Object> cache;

  public CacheStore(CacheProperties properties)
  {
    Preconditions.checkNotNull(properties.entryExpiryStrategy, "expiryType");

    CacheBuilder<Object, Object> cacheBuilder = CacheBuilder.newBuilder();
    if (properties.entryExpiryStrategy == ExpiryType.EXPIRE_AFTER_ACCESS) {
      cacheBuilder.expireAfterAccess(properties.entryExpiryDurationInMillis, TimeUnit.MILLISECONDS);
    }
    else if (properties.entryExpiryStrategy == ExpiryType.EXPIRE_AFTER_WRITE) {
      cacheBuilder.expireAfterWrite(properties.entryExpiryDurationInMillis, TimeUnit.MILLISECONDS);
    }
    cache = cacheBuilder.build();
    this.cleanupScheduler = Executors.newScheduledThreadPool(1);
    cleanupScheduler.scheduleAtFixedRate(new Runnable()
    {
      @Override
      public void run()
      {
        cache.cleanUp();
      }
    }, properties.cacheCleanupIntervalInMillis, properties.cacheCleanupIntervalInMillis, TimeUnit.MILLISECONDS);
  }

  @Override
  protected void setValueFor(Object key, Object value)
  {
    cache.put(key, value);
  }

  @Override
  void bulkSet(Map<Object, Object> data)
  {
    cache.asMap().putAll(data);
  }

  @Override
  protected Object getValueFor(Object key)
  {
    return cache.getIfPresent(key);
  }

  @Override
  protected void shutdownStore()
  {
    cleanupScheduler.shutdown();
  }

  /**
   * Strategies for time-based expiration of entries.
   */
  public static enum ExpiryType
  {
    /**
     * Only expire the entries after the specified duration has passed since the entry was last accessed by a read
     * or a write.
     */
    EXPIRE_AFTER_ACCESS,
    /**
     * Expire the entries after the specified duration has passed since the entry was created, or the most recent
     * replacement of the value.
     */
    EXPIRE_AFTER_WRITE
  }
}
