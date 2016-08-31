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
package com.datatorrent.benchmark.spillable;

import java.io.IOException;
import java.util.Map;
import java.util.Random;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.state.managed.Bucket;
import org.apache.apex.malhar.lib.state.spillable.SpillableByteArrayListMultimapImpl;
import org.apache.apex.malhar.lib.state.spillable.SpillableByteMapImpl;
import org.apache.apex.malhar.lib.state.spillable.SpillableTestUtils;
import org.apache.apex.malhar.lib.state.spillable.managed.ManagedStateSpillableStateStore;
import org.apache.apex.malhar.lib.utils.serde.LengthValueBuffer;
import org.apache.apex.malhar.lib.utils.serde.SerdeStringSlice;
import org.apache.apex.malhar.lib.utils.serde.SerdeStringWithLVBuffer;

import com.google.common.collect.Maps;

import com.datatorrent.lib.fileaccess.TFileImpl;
import com.datatorrent.netlet.util.Slice;


public class SpillableDSBenchmarkTest
{
  public static final transient Logger logger = LoggerFactory.getLogger(SpillableDSBenchmarkTest.class);
  protected static final transient int loopCount = 100000000;
  protected static final transient long oneMB = 1024*1024;
  protected static final transient int keySize = 100000;   
  protected static final transient int valueSize = 100000;
  protected static final transient int valuesPerKey = 100;
  protected static final int maxKeyLength = 100;
  protected static final int maxValueLength = 1000;
  
  protected static final int tuplesPerWindow = 10000;
  protected static final int checkPointWindows = 10;
  
  protected final transient Random random = new Random();
  
  protected String[] keys;
  protected String[] values;
  protected LengthValueBuffer buffer = new LengthValueBuffer();
  
  @Rule
  public SpillableTestUtils.TestMeta testMeta = new SpillableTestUtils.TestMeta();

  public static class OptimisedStateStore extends ManagedStateSpillableStateStore
  {
    protected long windowId;

    public void beginWindow(long windowId)
    {
      super.beginWindow(windowId);
      this.windowId = windowId;
    }

    @Override
    public void endWindow()
    {
      super.endWindow();
    }

    /**
     * - beforeCheckpoint() and other process method should be in same thread,
     * and no need lock
     */
    @Override
    public void beforeCheckpoint(long windowId)
    {
      Map<Long, Map<Slice, Bucket.BucketedValue>> flashData = Maps.newHashMap();

      for (Bucket bucket : buckets) {
        if (bucket != null) {
          Map<Slice, Bucket.BucketedValue> flashDataForBucket = bucket.checkpoint(windowId);
          if (!flashDataForBucket.isEmpty()) {
            flashData.put(bucket.getBucketId(), flashDataForBucket);
          }
        }
      }
      if (!flashData.isEmpty()) {
        try {
          getCheckpointManager().save(flashData, operatorContext.getId(), windowId, false);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }

        flashData.clear();
      }
    }
  }

  @Before
  public void setup()
  {
    keys = new String[keySize];
    for (int i = 0; i < keys.length; ++i) {
      keys[i] = this.randomString(maxKeyLength);
    }

    values = new String[valueSize];
    for (int i = 0; i < values.length; ++i) {
      values[i] = this.randomString(maxValueLength);
    }
  }
  
  

  @Test
  public void testSpillableMap()
  {
    byte[] ID1 = new byte[]{(byte)1};
    OptimisedStateStore store = new OptimisedStateStore();
    ((TFileImpl.DTFileImpl)store.getFileAccess()).setBasePath("target/temp");

    SerdeStringSlice keySerde = createKeySerde();
    SerdeStringSlice valueSerde = createValueSerde();


//    SpillableByteArrayListMultimapImpl<String, String> map = new SpillableByteArrayListMultimapImpl<String, String>(
//        store, ID1, 0L, keySerde, valueSerde, buffer);

    SpillableByteMapImpl<String, String> map = new SpillableByteMapImpl<String, String>(store, ID1, 0L, keySerde, valueSerde);
    store.setup(testMeta.operatorContext);
    map.setup(testMeta.operatorContext);

    final long startTime = System.currentTimeMillis();

    long windowId = 0;
    store.beginWindow(++windowId);
    map.beginWindow(windowId);

    int outputTimes = 0;
    for (int i = 0; i < loopCount; ++i) {
      putEntry(map);

      if (i % tuplesPerWindow == 0) {
        map.endWindow();
        store.endWindow();

        if(i % (tuplesPerWindow * checkPointWindows) == 0) {
          store.beforeCheckpoint(windowId);
          resetBuffer();
        }
        
        //next window
        store.beginWindow(++windowId);
        map.beginWindow(windowId);
      }

      long spentTime = System.currentTimeMillis() - startTime;
      if (spentTime > outputTimes * 5000) {
        ++outputTimes;
        logger.info("Spent {} mills for {} operation. average: {}, buffer size: {}, buffer capacity: {}", spentTime, i, i / spentTime, buffer.size(), buffer.capacity());
        checkEnvironment();
      }
    }
    long spentTime = System.currentTimeMillis() - startTime;

    logger.info("Spent {} mills for {} operation. average: {}", spentTime, loopCount,
        loopCount / spentTime);
  }

  /**
   * put the entry into the map
   * @param multiMap
   */
  public void putEntry(SpillableByteArrayListMultimapImpl<String, String> multiMap)
  {
    multiMap.put(keys[random.nextInt(keys.length)], values[random.nextInt(values.length)]);
  }
  
  public void putEntry(SpillableByteMapImpl<String, String> map)
  {
    map.put(keys[random.nextInt(keys.length)], values[random.nextInt(values.length)]);
  }

  public static final String characters = "0123456789ABCDEFGHIJKLMNOPKRSTUVWXYZabcdefghijklmopqrstuvwxyz";

  protected static final char[] text = new char[Math.max(maxKeyLength, maxValueLength)];

  public String randomString(int length)
  {
    for (int i = 0; i < length; i++) {
      text[i] = characters.charAt(random.nextInt(characters.length()));
    }
    return new String(text, 0, length);
  }
  
  public void checkEnvironment()
  {
    Runtime runtime = Runtime.getRuntime();

    long maxMemory = runtime.maxMemory();
    long allocatedMemory = runtime.totalMemory();
    long freeMemory = runtime.freeMemory();
    
    logger.info("freeMemory: {}M; allocatedMemory: {}M; maxMemory: {}M", freeMemory / oneMB,
        allocatedMemory / oneMB, maxMemory / oneMB);
  }

  protected SerdeStringSlice createKeySerde()
  {
    return new SerdeStringWithLVBuffer(buffer);
  }

  protected SerdeStringSlice createValueSerde()
  {
    return new SerdeStringWithLVBuffer(buffer);
  }

  protected void resetBuffer()
  {
    buffer.reset();
  }
}
