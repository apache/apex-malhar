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
import java.util.concurrent.Exchanger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Sets;

import com.datatorrent.lib.helper.OperatorContextTestHelper;

/**
 * Tests for {@link HdfsBucketStore}
 */
public class BucketManagerTest
{
  private static final String APPLICATION_PATH_PREFIX = "target/BucketManagerTest";
  private final static Exchanger<Long> eventBucketExchanger = new Exchanger<Long>();

  private static BucketManager<DummyEvent> manager;
  private static String applicationPath;
  private static HdfsBucketStore<DummyEvent> hdfsBucketStore;
  private static int bucket1 = 2875;
  private static int bucket2 = 2876;

  private static class TestStorageManagerListener implements BucketManager.Listener
  {

    @Override
    public void bucketLoaded(long bucketKey)
    {
      try {
        eventBucketExchanger.exchange(bucketKey);
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
    testRound(0);
    testRound(1);
  }

  @Test
  public void testBucketEviction() throws IOException, InterruptedException
  {
    manager.loadBucketData(new BucketManager.LoadCommand(bucket1));
    eventBucketExchanger.exchange(null);
    manager.loadBucketData(new BucketManager.LoadCommand(bucket2));
    eventBucketExchanger.exchange(null);
    Assert.assertTrue(manager.getBucket(bucket1) == null);
    Assert.assertNotNull(manager.getBucket(bucket2));
  }

  @Test
  public void testBucketDeletion() throws Exception
  {
    hdfsBucketStore.deleteBucket(bucket1);
    testRound(0);
  }

  private void testRound(int round) throws Exception
  {
    long now = System.currentTimeMillis();
    manager.loadBucketData(new BucketManager.LoadCommand(bucket1));
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

  @BeforeClass
  public static void setup() throws Exception
  {
    applicationPath = OperatorContextTestHelper.getUniqueApplicationPath(APPLICATION_PATH_PREFIX);
    hdfsBucketStore = new HdfsBucketStore<DummyEvent>(applicationPath, 0, 50, Sets.newHashSet(0), 0);
    manager = new BucketManager<DummyEvent>(true, 2880, 1, 1, 1);
    manager.startService(hdfsBucketStore, new TestStorageManagerListener());
  }

  @AfterClass
  public static void teardown() throws IOException
  {
    hdfsBucketStore.deleteBucket(bucket1);
    manager.shutdownService();
    Path root = new Path(applicationPath);
    FileSystem fs = FileSystem.get(root.toUri(), new Configuration());
    fs.delete(root, true);
  }


}
