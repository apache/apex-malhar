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

import javax.validation.constraints.Min;

/**
 * Properties and their default values which is used by {@link CacheStore} to create the cache.<br/>
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

  CacheStore.ExpiryType entryExpiryStrategy = CacheStore.ExpiryType.EXPIRE_AFTER_ACCESS;

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
  public void setEntryExpiryStrategy(CacheStore.ExpiryType expiryType)
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
