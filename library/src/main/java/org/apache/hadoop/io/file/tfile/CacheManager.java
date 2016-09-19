/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.io.file.tfile;

import java.lang.management.ManagementFactory;
import java.util.Collection;

import org.apache.hadoop.io.file.tfile.DTBCFile.Reader.BlockReader;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.Weigher;

/**
 * A single global managed cache
 * User can limit the cache size by num of entries, memory size (bytes) or percentage of total heap size
 * <br>
 * <br>
 * Please refer to <a href="https://code.google.com/p/guava-libraries/wiki/CachesExplained">Guava Cache</a> fir details
 * <br>
 * <br>
 * It keeps {@link String} as key and {@link BlockReader} as value
 *
 * @since 2.0.0
 */
public class CacheManager
{
  public static final int STRING_OVERHEAD = 64;

  public static final int BLOCK_READER_OVERHEAD = 368;

  public static final float DEFAULT_HEAP_MEMORY_PERCENTAGE = 0.25f;

  private static Cache<String, BlockReader> singleCache;

  private static boolean enableStats = false;

  public static final Cache<String, BlockReader> buildCache(CacheBuilder builder)
  {
    if (singleCache != null) {
      singleCache.cleanUp();
    }
    if (enableStats) {
      //todo: when we upgrade to a newer guava version we can use this
      // builder.recordStats();
    }
    singleCache = builder.build();
    return singleCache;
  }

  /**
   * (Re)Create the cache by limiting the maximum entries
   * @param concurrencyLevel
   * @param initialCapacity
   * @param maximunSize
   * @return The cache.
   */
  public static final Cache<String, BlockReader> createCache(int concurrencyLevel,int initialCapacity, int maximunSize)
  {
    CacheBuilder builder = CacheBuilder.newBuilder().concurrencyLevel(concurrencyLevel)
        .initialCapacity(initialCapacity).maximumSize(maximunSize);

    return buildCache(builder);
  }


  /**
   * (Re)Create the cache by limiting the memory(in bytes)
   * @param concurrencyLevel
   * @param initialCapacity
   * @param maximumMemory
   * @return The cache.
   */
  public static final Cache<String, BlockReader> createCache(int concurrencyLevel,int initialCapacity, long maximumMemory)
  {

    CacheBuilder builder = CacheBuilder.newBuilder().concurrencyLevel(concurrencyLevel).initialCapacity(initialCapacity)
        .maximumWeight(maximumMemory).weigher(new KVWeigher());

    return buildCache(builder);
  }

  /**
   * (Re)Create the cache by limiting percentage of the total heap memory
   * @param concurrencyLevel
   * @param initialCapacity
   * @param heapMemPercentage
   * @return The cache.
   */
  public static final Cache<String, BlockReader> createCache(int concurrencyLevel, int initialCapacity,
      float heapMemPercentage)
  {
    CacheBuilder builder = CacheBuilder.newBuilder()
        .concurrencyLevel(concurrencyLevel).initialCapacity(initialCapacity)
        .maximumWeight((long)(ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getMax() * heapMemPercentage))
        .weigher(new KVWeigher());
    return buildCache(builder);
  }

  public static final void createDefaultCache()
  {

    long availableMemory = (long)(ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getMax() * DEFAULT_HEAP_MEMORY_PERCENTAGE);
    CacheBuilder<String, BlockReader> builder = CacheBuilder.newBuilder().maximumWeight(availableMemory).weigher(
        new KVWeigher());

    singleCache = buildCache(builder);
  }

  public static final void put(String key, BlockReader blk)
  {
    if (singleCache == null) {
      createDefaultCache();
    }
    singleCache.put(key, blk);
  }

  public static final BlockReader get(String key)
  {
    if (singleCache == null) {
      return null;
    }
    return singleCache.getIfPresent(key);
  }

  public static final void invalidateKeys(Collection<String> keys)
  {
    if (singleCache != null) {
      singleCache.invalidateAll(keys);
    }
  }

  public static final long getCacheSize()
  {
    if (singleCache != null) {
      return singleCache.size();
    }
    return 0;
  }

  public static final class KVWeigher implements Weigher<String, BlockReader>
  {

    @Override
    public int weigh(String key, BlockReader value)
    {
      return (STRING_OVERHEAD + BLOCK_READER_OVERHEAD) +
          key.getBytes().length +
          value.getBlockDataInputStream().getBuf().length;
    }

  }

  @VisibleForTesting
  protected static Cache<String, BlockReader> getCache()
  {
    return singleCache;
  }

  public static final void setEnableStats(boolean enable)
  {
    enableStats = enable;
  }

  public static void main(String[] args)
  {

    //code to eitsmate the overhead of the instance of the key value objects
    // it depends on hbase file
//        ClassSize.estimateBase(Algorithm.class, true) +
//        ClassSize.estimateBase(RBlockState.class, true) +
//        ClassSize.estimateBase(ReusableByteArrayInputStream.class, true) +
//        ClassSize.estimateBase(BlockRegion.class, true));
//
//        ClassSize.estimateBase(String.class, true));
  }

}
