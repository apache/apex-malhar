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

package org.apache.hadoop.io.file.tfile;

import java.lang.management.ManagementFactory;

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
 */
public class CacheManager
{
  public static final int STRING_OVERHEAD = 64;
  
  public static final int BLOCK_READER_OVERHEAD = 368;
  
  public static final float DEFAULT_HEAP_MEMORY_PERCENTAGE = 0.25f;
  
  private static Cache<String, BlockReader> singleCache;
  
  
  /**
   * (Re)Create the cache by limiting the maximum entries
   * @param concurrencyLevel
   * @param initialCapacity
   * @param maximunSize
   * @return
   */
  public static final Cache<String, BlockReader> createCache(int concurrencyLevel,int initialCapacity, int maximunSize){
    singleCache = CacheBuilder.newBuilder().
        concurrencyLevel(concurrencyLevel).
        initialCapacity(initialCapacity).
        maximumSize(maximunSize).build();
    return singleCache;
  }
  
  
  /**
   * (Re)Create the cache by limiting the memory(in bytes)
   * @param concurrencyLevel
   * @param initialCapacity
   * @param maximumWeight
   * @return
   */
  public static final Cache<String, BlockReader> createCache(int concurrencyLevel,int initialCapacity, long maximumMemory){
    if (singleCache != null) {
      singleCache.cleanUp();
    }
    singleCache = CacheBuilder.newBuilder().
        concurrencyLevel(concurrencyLevel).
        initialCapacity(initialCapacity).
        maximumWeight(maximumMemory).weigher(new KVWeigher()).build();
    return singleCache;
  }
  
  /**
   * (Re)Create the cache by limiting percentage of the total heap memory
   * @param concurrencyLevel
   * @param initialCapacity
   * @param heapMemPercentage
   * @return
   */
  public static final Cache<String, BlockReader> createCache(int concurrencyLevel,int initialCapacity, float heapMemPercentage){
    if (singleCache != null) {
      singleCache.cleanUp();
    }
    singleCache = CacheBuilder.newBuilder().
        concurrencyLevel(concurrencyLevel).
        initialCapacity(initialCapacity).
        maximumWeight((long) (ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getMax() * heapMemPercentage)).weigher(new KVWeigher()).build();
    return singleCache;
  }
  
  public static final void createDefaultCache(){
    if (singleCache != null) {
      singleCache.cleanUp();
    }
    long availableMemory = (long) (ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getMax() * DEFAULT_HEAP_MEMORY_PERCENTAGE);
    singleCache = CacheBuilder.newBuilder().maximumWeight(availableMemory).weigher(new KVWeigher()).build();
  }
  
  public static final void put(String key, BlockReader blk){
    if (singleCache == null) {
      createDefaultCache();
    }
    singleCache.put(key, blk);
  }
  
  public static final BlockReader get(String key){
    if (singleCache == null) {
      return null;
    }
    return singleCache.getIfPresent(key);
  }
  
  
  public static final class KVWeigher implements Weigher<String, BlockReader> {
    
    @Override
    public int weigh(String key, BlockReader value)
    {
      return (STRING_OVERHEAD + BLOCK_READER_OVERHEAD) + 
          key.getBytes().length + 
          value.getBlockDataInputStream().getBuf().length;
    }
    
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
