/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.bucket;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Exchanger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import com.datatorrent.lib.helper.OperatorContextTestHelper;

/**
 * Tests for {@link com.datatorrent.lib.bucket.BucketManagerImpl}
 */
public class BucketManagerTest
{
  private static final String APPLICATION_PATH_PREFIX = "target/BucketManagerTest";
  private final static Exchanger<Long> eventBucketExchanger = new Exchanger<Long>();

  private static BucketManagerImpl<DummyEvent> manager;
  private static String applicationPath;
  private static int bucket1 = 2875;
  private static int bucket2 = 2876;

  static class TestStorageManagerListener implements BucketManager.Listener<DummyEvent>
  {

    @Override
    public void bucketLoaded(Bucket<DummyEvent> bucket)
    {
      try {
        eventBucketExchanger.exchange(bucket.bucketKey);
      }
      catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void bucketOffLoaded(long bucketKey)
    {
    }

  }

  @Test
  public void testLoadAndSave() throws Exception
  {
    for (int i = 0; i < 100; i++) {
      testRound(i);
    }
  }

  private void testRound(int round) throws Exception
  {
    long now = System.currentTimeMillis();
    manager.loadBucketData(bucket1);
    eventBucketExchanger.exchange(null);
    for (int i = round * 10; i < (round * 10) + 10; i++) {
      DummyEvent dummyEvent = new DummyEvent(i, now);
      manager.newEvent(bucket1, dummyEvent);
    }
    manager.endWindow(round);
    Bucket<DummyEvent> bucket = manager.getBucket(bucket1);
    Assert.assertNotNull(bucket);
    Assert.assertEquals("no of events", (round + 1) * 10, bucket.countOfWrittenEvents());
  }

  @Test
  public void testBucketEviction() throws IOException, InterruptedException
  {
    manager.loadBucketData(bucket1);
    eventBucketExchanger.exchange(null);
    manager.loadBucketData(bucket2);
    eventBucketExchanger.exchange(null);
    Assert.assertTrue(manager.getBucket(bucket1) == null);
    Assert.assertNotNull(manager.getBucket(bucket2));
  }

  @Test
  public void test2BucketEviction() throws InterruptedException
  {
    for (int i = 0; i < 100; i++) {
      manager.loadBucketData(i);
      eventBucketExchanger.exchange(null);
    }

    for (int i = 0; i < 100; i++) {
      if (i != 99) {
        Assert.assertTrue(manager.getBucket(i) == null);
      }
      else {
        Assert.assertNotNull(manager.getBucket(i));
      }
    }
  }


  @BeforeClass
  public static void setup() throws Exception
  {
    applicationPath = OperatorContextTestHelper.getUniqueApplicationPath(APPLICATION_PATH_PREFIX);

    Map<String, Object> parameters = Maps.newHashMap();
    parameters.put(HdfsBucketStore.APP_PATH, applicationPath);
    parameters.put(HdfsBucketStore.OPERATOR_ID, 0);
    parameters.put(HdfsBucketStore.PARTITION_KEYS, Sets.newHashSet(0));
    parameters.put(HdfsBucketStore.PARTITION_MASK, 0);

    manager = new BucketManagerImpl<DummyEvent>();
    manager.setNoOfBuckets(2880);
    manager.setNoOfBucketsInMemory(1);
    manager.setMaxNoOfBucketsInMemory(1);
    manager.setMillisPreventingBucketEviction(1);
    manager.startService(new Context(parameters), new TestStorageManagerListener());
  }

  @AfterClass
  public static void teardown() throws IOException
  {
    manager.shutdownService();
    Path root = new Path(applicationPath);
    FileSystem fs = FileSystem.get(root.toUri(), new Configuration());
    fs.delete(root, true);
  }
}
