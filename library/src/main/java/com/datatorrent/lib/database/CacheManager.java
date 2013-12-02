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

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

/**
 * <br>This creates a {@link LoadingCache} that stores db result for certain time.</br>
 * <br>
 * The Cache has a maximum size and it may evict an entry before this limit is exceeded.
 * The entries in the cache expire after the specified expiry-duration.
 * </br>
 *
 * @since 0.9.1
 */
public class CacheManager
{
  private transient final CacheUser cacheUser;

  private transient LoadingCache<Object, Object> cache;

  private transient ScheduledExecutorService cleanupScheduler;

  /**
   * @param user       implementation of {@link CacheUser} which has the onus of loading the value of the key
   *                   from database.
   * @param properties {@link CacheProperties} which define the cache.
   */
  public CacheManager(CacheUser user, CacheProperties properties)
  {
    this.cacheUser = Preconditions.checkNotNull(user, "cache user");

    Preconditions.checkNotNull(properties.entryExpiryStrategy, "expiryType");

    CacheBuilder<Object, Object> cacheBuilder = CacheBuilder.newBuilder();
    if (properties.entryExpiryStrategy == ExpiryType.EXPIRE_AFTER_ACCESS) {
      cacheBuilder.expireAfterAccess(properties.entryExpiryDurationInMillis, TimeUnit.MILLISECONDS);
    }
    else if (properties.entryExpiryStrategy == ExpiryType.EXPIRE_AFTER_WRITE) {
      cacheBuilder.expireAfterWrite(properties.entryExpiryDurationInMillis, TimeUnit.MILLISECONDS);
    }

    this.cache = cacheBuilder.maximumSize(properties.maxCacheSize)
      .build(new CacheLoader<Object, Object>()
      {
        @Override
        public Object load(Object k) throws Exception
        {
          Object val = cacheUser.fetchValueFromDatabase(k);
          if (val == null) {
            cache.put(k, val);
          }
          return val;
        }
      });
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

  /**
   * shutsdown the cache cleanup scheduler
   */
  public void shutdownCleanupScheduler()
  {
    cleanupScheduler.shutdown();
  }

  /**
   * @return the underlying cache
   */
  public LoadingCache<Object, Object> getCache()
  {
    return cache;
  }

  /**
   * <br>
   * The user of CacheManager should implement this interface to customise the
   * retrieval of a particular key from the database when it is not found in the cache.
   * </br>
   */
  public static interface CacheUser
  {
    Object fetchValueFromDatabase(Object key);
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
