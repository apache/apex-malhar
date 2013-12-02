package com.datatorrent.lib.database;

import javax.validation.constraints.Min;

/**
 * <br>Properties and their default values which is used by {@link CacheManager} to create the cache.</br>
 *
 * @since 0.9.1
 */
public class CacheProperties
{

  @Min(0)
  long maxCacheSize = 2000;

  @Min(0)
  int entryExpiryDurationInMillis = 60000; //1 minute

  @Min(0)
  int cacheCleanupIntervalInMillis = 60500; //.5 seconds after entries are expired

  CacheManager.ExpiryType entryExpiryStrategy = CacheManager.ExpiryType.EXPIRE_AFTER_ACCESS;

  /**
   * Sets the max size of cache.
   * @param maxCacheSize the max size of cache in memory.
   */
  public void setMaxCacheSize(long maxCacheSize)
  {
    this.maxCacheSize = maxCacheSize;
  }

  /**
   * Sets the cache entry expiry strategy.
   * @param expiryType the cache entry expiry strategy.
   */
  public void setEntryExpiryStrategy(CacheManager.ExpiryType expiryType)
  {
    this.entryExpiryStrategy = expiryType;
  }

  /**
   * Sets the expiry time of cache entries in millis.
   * @param durationInMillis the duration after which a cache entry is expired.
   */
  public void setEntryExpiryDurationInMillis(int durationInMillis)
  {
    this.entryExpiryDurationInMillis = durationInMillis;
  }

  /**
   * Sets the interval at which cache is cleaned up regularly.
   * @param durationInMillis the duration after which cache is cleaned up regularly.
   */
  public void setCacheCleanupInMillis(int durationInMillis)
  {
    this.cacheCleanupIntervalInMillis = durationInMillis;
  }
}
