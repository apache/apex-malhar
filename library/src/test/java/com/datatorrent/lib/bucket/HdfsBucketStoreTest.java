/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
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
package com.datatorrent.lib.bucket;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import com.datatorrent.lib.helper.OperatorContextTestHelper;

public class HdfsBucketStoreTest
{

  public static class TestMeta extends TestWatcher
  {
    protected final String APPLICATION_PATH_PREFIX = "target/HdfsBucketStoreTest";
    protected final int TOTAL_BUCKETS = 1000;
    protected String applicationPath;
    protected Path rootBucketPath;
    protected HdfsBucketStore<DummyEvent> bucketStore;
    protected Map<Integer, Map<Object, DummyEvent>> data = Maps.newHashMap();
    protected FileSystem fs;
    protected BucketStoreTestsUtil util;

    @Override
    protected void starting(Description description)
    {
      applicationPath = OperatorContextTestHelper.getUniqueApplicationPath(APPLICATION_PATH_PREFIX);
      bucketStore = getBucketStore();
      bucketStore.setup();
      util = new BucketStoreTestsUtil(this);

      for (int bucketIdx = 0; bucketIdx < 2; bucketIdx++) {
        Map<Object, DummyEvent> bucketData = Maps.newHashMap();
        data.put(bucketIdx, bucketData);

        for (int i = 0; i < 10; i++) {
          DummyEvent event = new DummyEvent(i, System.currentTimeMillis());
          bucketData.put(event.getEventKey(), event);
        }
      }
      rootBucketPath = new Path(bucketStore.bucketRoot);
      try {
        fs = FileSystem.newInstance(rootBucketPath.toUri(), new Configuration());
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    protected void finished(Description description)
    {
      Path root = new Path(applicationPath);
      try {
        fs.delete(root, true);
        if (fs != null) {
          fs.close();
        }
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    protected HdfsBucketStore<DummyEvent> getBucketStore()
    {
      HdfsBucketStore<DummyEvent> lBucketStore = new HdfsBucketStore<DummyEvent>();
      lBucketStore.setNoOfBuckets(TOTAL_BUCKETS);
      lBucketStore.setWriteEventKeysOnly(true);
      lBucketStore.setConfiguration(7, applicationPath, Sets.newHashSet(0), 0);
      return lBucketStore;
    }
  }

  @Rule
  public TestMeta testMeta = new TestMeta();

  @Test
  public void testStoreBucketData() throws Exception
  {
    testMeta.util.storeBucket(0);
  }

  @Test
  public void testFetchBucket() throws Exception
  {
    testMeta.util.fetchBucket(0);
  }

  @Test
  public void testDeleteBucket() throws Exception
  {
    testMeta.util.deleteBucket(0);
  }

  @Test
  public void testValuePersistence() throws Exception
  {
    testMeta.bucketStore.setWriteEventKeysOnly(false);
    testMeta.bucketStore.setup();
    DummyEvent newEvent = new DummyEvent(0, System.currentTimeMillis());
    //store data for window 0
    testMeta.util.storeBucket(0);

    //populate data for window 1
    testMeta.data = Maps.newHashMap();
    Map<Object, DummyEvent> bucketData = Maps.newHashMap();
    bucketData.put(newEvent.getEventKey(), newEvent);
    testMeta.data.put(0, bucketData);
    //store data for window 1
    testMeta.util.storeBucket(1);

    Map<Object, DummyEvent> fetchBucket = testMeta.bucketStore.fetchBucket(0);
    DummyEvent retrievedEvent = fetchBucket.get(newEvent.getEventKey());

    Assert.assertTrue("latest value", retrievedEvent.equals(newEvent));
  }

  public static class BucketStoreTestsUtil
  {
    protected final TestMeta meta;

    public BucketStoreTestsUtil(TestMeta meta)
    {
      this.meta = meta;
    }

    public void storeBucket(long window) throws Exception
    {
      meta.bucketStore.storeBucketData(window, 0, meta.data);
      Assert.assertTrue(meta.fs.exists(meta.rootBucketPath));
    }

    public void fetchBucket(long window) throws Exception
    {
      meta.bucketStore.storeBucketData(window, 0, meta.data);
      Map<Object, DummyEvent> fetchedData = meta.bucketStore.fetchBucket(0);

      for (Map.Entry<Object, DummyEvent> entry : fetchedData.entrySet()) {
        Assert.assertTrue(entry.getValue() == null);
        Assert.assertTrue(meta.data.get(0).containsKey(entry.getKey()));
      }

      fetchedData = meta.bucketStore.fetchBucket(1);
      for (Map.Entry<Object, DummyEvent> entry : fetchedData.entrySet()) {
        Assert.assertTrue(entry.getValue() == null);
        Assert.assertTrue(meta.data.get(1).containsKey(entry.getKey()));
      }
    }

    public void deleteBucket(long window) throws Exception
    {
      meta.bucketStore.storeBucketData(window, 0, meta.data);
      meta.bucketStore.deleteBucket(1);
      Map<Object, DummyEvent> fetchedData = meta.bucketStore.fetchBucket(1);
      Assert.assertNotNull(fetchedData);
      Assert.assertTrue(fetchedData.size() == 0);
      deleteFsPath(meta.rootBucketPath);
    }

    void deleteFsPath(Path path) throws IOException
    {
      meta.fs.delete(path, true);
    }

    boolean bucketExists(int fileId)
    {
      Path bucketPath = new Path(meta.applicationPath + HdfsBucketStore.PATH_SEPARATOR + 7 + HdfsBucketStore.PATH_SEPARATOR
        + fileId);
      try {
        return meta.fs.exists(bucketPath);
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private static final Logger logger = LoggerFactory.getLogger(HdfsBucketStoreTest.class);
}