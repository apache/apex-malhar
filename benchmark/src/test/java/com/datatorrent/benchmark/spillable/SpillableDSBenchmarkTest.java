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
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.state.managed.Bucket;
import org.apache.apex.malhar.lib.state.spillable.SpillableByteArrayListMultimapImpl;
import org.apache.apex.malhar.lib.state.spillable.SpillableTestUtils;
import org.apache.apex.malhar.lib.state.spillable.managed.ManagedStateSpillableStateStore;
import org.apache.apex.malhar.lib.utils.serde.SerdeStringSlice;

import com.google.common.collect.Maps;

import com.datatorrent.lib.fileaccess.TFileImpl;
import com.datatorrent.netlet.util.Slice;


public class SpillableDSBenchmarkTest
{
  public static final transient Logger logger = LoggerFactory.getLogger(SpillableDSBenchmarkTest.class);
  protected static final transient int loopCount = 100000000;
  protected static final transient long oneMB = 1024*1024;
  protected static final transient int keySize = 1000000;
  protected static final transient int valueSize = 100000000;
  
  protected final transient Random random = new Random();
  
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
      beforeCheckpoint(this.windowId);
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

  @Test
  public void testSpillableMutimap()
  {
    testSpillableMutimap(true);
  }

  public void testSpillableMutimap(boolean useLvBuffer)
  {
    byte[] ID1 = new byte[]{(byte)1};
    OptimisedStateStore store = new OptimisedStateStore();
    ((TFileImpl.DTFileImpl)store.getFileAccess()).setBasePath("target/temp");

    SerdeStringSlice keySerde = createKeySerde();
    ;
    SerdeStringSlice valueSerde = createValueSerde();
    ;

    SpillableByteArrayListMultimapImpl<String, String> multiMap = new SpillableByteArrayListMultimapImpl<String, String>(
        store, ID1, 0L, keySerde, valueSerde);

    store.setup(testMeta.operatorContext);
    multiMap.setup(testMeta.operatorContext);

    String[] strs = new String[]{"123", "45678", "abcdef", "dfaqecdgr"};

    final long startTime = System.currentTimeMillis();

    long windowId = 0;
    store.beginWindow(++windowId);
    multiMap.beginWindow(windowId);

    int outputTimes = 0;
    for (int i = 0; i < loopCount; ++i) {
      putEntry(multiMap);

      if (i % 100000 == 0) {
        multiMap.endWindow();
        store.endWindow();

        //NOTES: it will great impact the performance if the size of buffer is too large
        resetBuffer();

        //next window
        store.beginWindow(++windowId);
        multiMap.beginWindow(windowId);
      }

      long spentTime = System.currentTimeMillis() - startTime;
      if (spentTime > outputTimes * 60000) {
        ++outputTimes;
        logger.info("Spent {} mills for {} operation. average: {}", spentTime, strs.length * i,
            strs.length * i / spentTime);
        checkEnvironment();
      }

    }
    long spentTime = System.currentTimeMillis() - startTime;

    logger.info("Spent {} mills for {} operation. average: {}", spentTime, strs.length * loopCount,
        strs.length * loopCount / spentTime);
  }

  /**
   * put the entry into the map
   * @param multiMap
   */
  public void putEntry(SpillableByteArrayListMultimapImpl<String, String> multiMap)
  {
    multiMap.put(String.valueOf(random.nextInt(keySize)), String.valueOf(random.nextInt(valueSize)));
  }

  public void checkEnvironment()
  {
    Runtime runtime = Runtime.getRuntime();

    long maxMemory = runtime.maxMemory();
    long allocatedMemory = runtime.totalMemory();
    long freeMemory = runtime.freeMemory();
    
    logger.info("freeMemory: {}M; allocatedMemory: {}M; maxMemory: {}M", freeMemory / oneMB,
        allocatedMemory / oneMB, maxMemory / oneMB);
    
    Assert.assertTrue("Used up all memory.", maxMemory - allocatedMemory > oneMB);
  }

  protected SerdeStringSlice createKeySerde()
  {
    return new SerdeStringSlice();
  }

  protected SerdeStringSlice createValueSerde()
  {
    return new SerdeStringSlice();
  }

  protected void resetBuffer()
  {
  }
}
