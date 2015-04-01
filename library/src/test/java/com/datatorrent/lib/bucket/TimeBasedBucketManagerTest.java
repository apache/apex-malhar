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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Sets;

import com.datatorrent.lib.helper.OperatorContextTestHelper;

public class TimeBasedBucketManagerTest
{
  private static final String APPLICATION_PATH_PREFIX = "target/TimeBasedBucketManagerTest";
  private static final long BUCKET_SPAN = 60000; //1 minute

  private static TestBucketManager<DummyEvent> manager;
  private static String applicationPath;

  private static class TestBucketManager<T extends Event & Bucketable> extends TimeBasedBucketManagerImpl<T>
  {
    TestBucketManager()
    {
      super();
    }
  }

  @Test
  public void testExpiration() throws InterruptedException
  {
    DummyEvent event1 = new DummyEvent(1, manager.startOfBucketsInMillis + 10 * BUCKET_SPAN);
    long bucket1 = manager.getBucketKeyFor(event1);
    DummyEvent event2 = new DummyEvent(1, manager.startOfBucketsInMillis + (manager.noOfBuckets + 10) * BUCKET_SPAN);
    long bucket2 = manager.getBucketKeyFor(event2);
    Assert.assertEquals("bucket index", bucket1 % manager.noOfBuckets, bucket2 % manager.noOfBuckets);

    bucket1 = manager.getBucketKeyFor(event1);
    Assert.assertEquals("expired event", bucket1, -1);

    long rBucket2 = manager.getBucketKeyFor(event2);
    Assert.assertEquals("valid event", bucket2, rBucket2);
  }

  @Test
  public void testClone() throws CloneNotSupportedException, InterruptedException
  {
    TimeBasedBucketManagerImpl<DummyEvent> clonedManager = manager.clone();
    Assert.assertNotNull(clonedManager);
    Assert.assertTrue(clonedManager.bucketStore.equals(manager.bucketStore));
    Assert.assertTrue(clonedManager.writeEventKeysOnly==manager.writeEventKeysOnly);
    Assert.assertTrue(clonedManager.noOfBuckets==manager.noOfBuckets);
    Assert.assertTrue(clonedManager.noOfBucketsInMemory==manager.noOfBucketsInMemory);
    Assert.assertTrue(clonedManager.maxNoOfBucketsInMemory==manager.maxNoOfBucketsInMemory);
    Assert.assertTrue(clonedManager.millisPreventingBucketEviction== manager.millisPreventingBucketEviction);
    Assert.assertTrue(clonedManager.committedWindow==manager.committedWindow);
    Assert.assertTrue(clonedManager.getMaxTimesPerBuckets().length== manager.getMaxTimesPerBuckets().length);
  }

  @BeforeClass
  public static void setup() throws Exception
  {
    applicationPath = OperatorContextTestHelper.getUniqueApplicationPath(APPLICATION_PATH_PREFIX);
    manager = new TestBucketManager<DummyEvent>();
    manager.setBucketSpanInMillis(BUCKET_SPAN);
    ExpirableHdfsBucketStore<DummyEvent> bucketStore = new ExpirableHdfsBucketStore<DummyEvent>();
    manager.setBucketStore(bucketStore);
    bucketStore.setConfiguration(0, applicationPath, Sets.newHashSet(0), 0);
    bucketStore.setup();
    manager.startService(new BucketManagerTest.TestStorageManagerListener());
  }

  @AfterClass
  public static void teardown() throws IOException
  {
    manager.shutdownService();
    Path root = new Path(applicationPath);
    FileSystem fs = FileSystem.newInstance(root.toUri(), new Configuration());
    fs.delete(root, true);
  }
}
