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

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.io.file.tfile.DTBCFile.Reader.BlockReader;
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

  private static Cache<String, BlockReader> SINGLE_CACHE;

  private static boolean ENABLE_STATS = false;

  @SuppressWarnings("unchecked")
  public static synchronized Cache<String, BlockReader> buildCache(CacheBuilder builder)
  {
    if (SINGLE_CACHE != null) {
      SINGLE_CACHE.cleanUp();
    }
    if (ENABLE_STATS) {
      //todo: when we upgrade to a newer guava version we can use this
      //SINGLE_CACHE.recordStats();
    }
    SINGLE_CACHE = builder.build();
    return SINGLE_CACHE;
  }

  /**
   * (Re)Create the cache by limiting the maximum entries
   * @param concurrencyLevel
   * @param initialCapacity
   * @param maximunSize
   * @return The cache.
   */
  public static Cache<String, BlockReader> createCache(int concurrencyLevel, int initialCapacity, int maximunSize)
  {
    CacheBuilder builder = CacheBuilder.newBuilder().
        concurrencyLevel(concurrencyLevel).
        initialCapacity(initialCapacity).
        maximumSize(maximunSize);

    return buildCache(builder);
  }


  /**
   * (Re)Create the cache by limiting the memory(in bytes)
   * @param concurrencyLevel
   * @param initialCapacity
   * @param maximumMemory
   * @return The cache.
   */
  public static final Cache<String, BlockReader> createCache(int concurrencyLevel, int initialCapacity, long maximumMemory)
  {
    CacheBuilder builder = CacheBuilder.newBuilder().
        concurrencyLevel(concurrencyLevel).
        initialCapacity(initialCapacity).
        maximumWeight(maximumMemory).weigher(new KVWeigher());

    return buildCache(builder);
  }

  /**
   * (Re)Create the cache by limiting percentage of the total heap memory
   * @param concurrencyLevel
   * @param initialCapacity
   * @param heapMemPercentage
   * @return The cache.
   */
  public static Cache<String, BlockReader> createCache(int concurrencyLevel, int initialCapacity, float heapMemPercentage)
  {
    CacheBuilder builder = CacheBuilder.newBuilder().
        concurrencyLevel(concurrencyLevel).
        initialCapacity(initialCapacity).
        maximumWeight((long) (ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getMax() * heapMemPercentage)).weigher(new KVWeigher());
    return buildCache(builder);
  }

  public static void createDefaultCache()
  {
    long availableMemory = (long)(ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getMax() * DEFAULT_HEAP_MEMORY_PERCENTAGE);
    CacheBuilder<String, BlockReader> builder = CacheBuilder.newBuilder().maximumWeight(availableMemory).weigher(new KVWeigher());

    SINGLE_CACHE = buildCache(builder);
  }

  public static void put(String key, BlockReader blk)
  {
    if (SINGLE_CACHE == null) {
      createDefaultCache();
    }
    SINGLE_CACHE.put(key, blk);
  }

  public static BlockReader get(String key)
  {
    if (SINGLE_CACHE == null) {
      return null;
    }
    return SINGLE_CACHE.getIfPresent(key);
  }

  public static void invalidateKeys(Collection<String> keys)
  {
    if (SINGLE_CACHE != null)
      SINGLE_CACHE.invalidateAll(keys);
  }

  public static long getCacheSize()
  {
    if (SINGLE_CACHE != null)
      return SINGLE_CACHE.size();
    return 0;
  }

  public static class KVWeigher implements Weigher<String, BlockReader>
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
    return SINGLE_CACHE;
  }

  /**
   * This is not working right now.
   * @param enable enable stats
   */
  public static void setEnableStats(boolean enable)
  {
    ENABLE_STATS = enable;
  }

  public static void main(String[] args)
  {

    //code to eitsmate the overhead of the instance of the key value objects
    // it depends on hbase file
//    System.out.println(ClassSize.estimateBase(BlockReader.class, true) +
//        ClassSize.estimateBase(Algorithm.class, true) +
//        ClassSize.estimateBase(RBlockState.class, true) +
//        ClassSize.estimateBase(ReusableByteArrayInputStream.class, true) +
//        ClassSize.estimateBase(BlockRegion.class, true));
//
//    System.out.println(
//        ClassSize.estimateBase(String.class, true));
  }

}
