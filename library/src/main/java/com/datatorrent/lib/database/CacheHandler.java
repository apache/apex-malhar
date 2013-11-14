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

import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import java.sql.SQLException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * <br>This creates a {@link LoadingCache} that stores db result for certain time.</br>
 * <br>
 * The Cache has a maximum size and it may evict an entry before this limit is exceeded.
 * The entries in the cache expire after the specified expiry-duration.
 * </br>
 *
 * @param <K> type of keys in the cache </K>
 */
public class CacheHandler<K>
{
  private transient final CacheUser<K> cacheUser;

  private transient LoadingCache<K, Object> cache;

  private transient ScheduledExecutorService cleanupScheduler;

  /**
   * @param user                         implementation of {@link CacheUser} which has the onus of loading the value of the key
   *                                     from database.
   * @param maxCacheSize                 maximum cache capacity.
   * @param expiryType                   entries expiry strategy.
   * @param expiryDurationInMillis       each entry will be expired when this duration has elapsed after the entry'creation,
   *                                     most recent replacement, or its last access based on the expiry type.
   * @param cacheCleanupIntervalInMillis duration in millis at which the cache is regulary cleaned - expired entries are
   *                                     removed.
   */
  public CacheHandler(CacheUser<K> user, long maxCacheSize, ExpiryType expiryType, int expiryDurationInMillis,
                      int cacheCleanupIntervalInMillis)
  {
    this.cacheUser = Preconditions.checkNotNull(user, "cache user");

    Preconditions.checkNotNull(expiryType, "expiryType");

    CacheBuilder<Object, Object> cacheBuilder = CacheBuilder.newBuilder();
    if (expiryType == ExpiryType.EXPIRE_AFTER_ACCESS)
      cacheBuilder.expireAfterAccess(expiryDurationInMillis, TimeUnit.MILLISECONDS);
    else if (expiryType == ExpiryType.EXPIRE_AFTER_WRITE)
      cacheBuilder.expireAfterWrite(expiryDurationInMillis, TimeUnit.MILLISECONDS);

    this.cache = cacheBuilder.maximumSize(maxCacheSize)
      .build(new CacheLoader<K, Object>()
      {
        @Override
        public Object load(K k) throws Exception
        {
          Object val = cacheUser.fetchKeyFromDatabase(k);
          if (val == null)
            cache.put(k, val);
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
    }, cacheCleanupIntervalInMillis, cacheCleanupIntervalInMillis, TimeUnit.MILLISECONDS);
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
  public LoadingCache<K, Object> getCache()
  {
    return cache;
  }

  /**
   * <br>
   * The user of CacheHandler should implement this interface to customise the
   * retrieval of a particular key from the database when it is not found in the cache.
   * </br>
   *
   * @param <K> type of keys int the cache </K>
   */
  public static interface CacheUser<K>
  {
    Object fetchKeyFromDatabase(K key) throws SQLException;
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
