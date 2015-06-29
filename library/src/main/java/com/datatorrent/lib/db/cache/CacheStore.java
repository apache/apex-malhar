/**
 * Copyright (C) 2015 DataTorrent, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.db.cache;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Lists;

/**
 * A {@link CacheManager.Primary} which keeps key/value pairs in memory.<br/>
 *
 * Properties of the cache store:<br/>
 * <ul>
 * <li>Transient: It is not checkpointed.</li>
 * <li>Max Cache Size: it starts evicting entries before this limit is exceeded.</li>
 * <li>Entry expiry time: the entries epire after the specified duration.</li>
 * <li>Cache cleanup interval: the interval at which the cache is cleaned up of expired entries periodically.</li>
 * </ul>
 *
 * @since 0.9.2
 */
public class CacheStore implements CacheManager.Primary
{

  @Min(0)
  protected long maxCacheSize = 2000;

  @Min(0)
  protected int entryExpiryDurationInMillis = 60000; //1 minute

  @Min(0)
  protected int cacheCleanupIntervalInMillis = 60500; //.5 seconds after entries are expired

  @NotNull
  protected ExpiryType entryExpiryStrategy = ExpiryType.EXPIRE_AFTER_ACCESS;

  private transient ScheduledExecutorService cleanupScheduler;
  private transient Cache<Object, Object> cache;
  private transient boolean open;


  @Override
  public void put(Object key, Object value)
  {
    cache.put(key, value);
  }

  @Override
  public Set<Object> getKeys()
  {
    return cache.asMap().keySet();
  }

  @Override
  public void putAll(Map<Object, Object> data)
  {
    cache.asMap().putAll(data);
  }

  @Override
  public void remove(Object key)
  {
    cache.invalidate(key);
  }

  @Override
  public Object get(Object key)
  {
    return cache.getIfPresent(key);
  }

  @Override
  public List<Object> getAll(List<Object> keys)
  {
    List<Object> values = Lists.newArrayList();
    for (Object key : keys) {
      values.add(cache.getIfPresent(key));
    }
    return values;
  }

  @Override
  public void connect() throws IOException
  {
    open = true;

    CacheBuilder<Object, Object> cacheBuilder = CacheBuilder.newBuilder();
    if (entryExpiryStrategy == ExpiryType.EXPIRE_AFTER_ACCESS) {
      cacheBuilder.expireAfterAccess(entryExpiryDurationInMillis, TimeUnit.MILLISECONDS);
    }
    else if (entryExpiryStrategy == ExpiryType.EXPIRE_AFTER_WRITE) {
      cacheBuilder.expireAfterWrite(entryExpiryDurationInMillis, TimeUnit.MILLISECONDS);
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
    }, cacheCleanupIntervalInMillis, cacheCleanupIntervalInMillis, TimeUnit.MILLISECONDS);
  }

  @Override
  public boolean isConnected()
  {
    return open;
  }

  @Override
  public void disconnect() throws IOException
  {
    open = false;
    cleanupScheduler.shutdown();
  }

  /**
   * Sets the max size of cache.
   *
   * @param maxCacheSize the max size of cache in memory.
   */
  public void setMaxCacheSize(long maxCacheSize)
  {
    this.maxCacheSize = maxCacheSize;
  }

  /**
   * Sets the cache entry expiry strategy.
   *
   * @param expiryType the cache entry expiry strategy.
   */
  public void setEntryExpiryStrategy(ExpiryType expiryType)
  {
    this.entryExpiryStrategy = expiryType;
  }

  /**
   * Sets the expiry time of cache entries in millis.
   *
   * @param durationInMillis the duration after which a cache entry is expired.
   */
  public void setEntryExpiryDurationInMillis(int durationInMillis)
  {
    this.entryExpiryDurationInMillis = durationInMillis;
  }

  /**
   * Sets the interval at which cache is cleaned up regularly.
   *
   * @param durationInMillis the duration after which cache is cleaned up regularly.
   */
  public void setCacheCleanupInMillis(int durationInMillis)
  {
    this.cacheCleanupIntervalInMillis = durationInMillis;
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
