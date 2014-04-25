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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import com.datatorrent.lib.helper.OperatorContextTestHelper;

public class BucketStoreTests
{
  private final String filepath = "src/test/resources/test_data/dt_spend";
  private final String APPLICATION_PATH_PREFIX = "target/HdfsBucketStoreTest";
  private final int TOTAL_BUCKETS = 1000;
  private String applicationPath;
  private Path rootBucketPath;
  private HdfsBucketStore<DummyEvent> bucketStore;
  private Map<Integer, Map<Object, DummyEvent>> data = Maps.newHashMap();
  private FileSystem fs;

  BucketStoreTests()
  {
  }

  void setup(HdfsBucketStore<DummyEvent> store)
  {
    applicationPath = OperatorContextTestHelper.getUniqueApplicationPath(APPLICATION_PATH_PREFIX);
    bucketStore = store;
    bucketStore.setNoOfBuckets(TOTAL_BUCKETS);
    bucketStore.setWriteEventKeysOnly(true);
    bucketStore.setConfiguration(7, applicationPath, Sets.newHashSet(0), 0);
    bucketStore.setup();

    for (int bucketIdx = 0; bucketIdx < 2; bucketIdx++) {
      Map<Object, DummyEvent> bucketData = Maps.newHashMap();
      data.put(bucketIdx, bucketData);

      for (int i = 0; i < 10; i++) {
        DummyEvent event = new DummyEvent(i, System.currentTimeMillis());
        bucketData.put(event.getEventKey(), event);
      }
    }
    rootBucketPath = new Path(applicationPath + HdfsBucketStore.PATH_SEPARATOR + 7);
    try {
      fs = FileSystem.newInstance(rootBucketPath.toUri(), new Configuration());
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  void tearDown() throws Exception
  {
    Path root = new Path(applicationPath);
    fs.delete(root, true);
    if (fs != null) {
      fs.close();
    }
  }

  void testStoreBucketData() throws Exception
  {
    bucketStore.storeBucketData(0, 0, data);
    Assert.assertTrue(fs.exists(rootBucketPath));
  }

  void testFetchBucket() throws Exception
  {
    bucketStore.storeBucketData(0, 0, data);
    Map<Object, DummyEvent> fetchedData = bucketStore.fetchBucket(0);

    for (Map.Entry<Object, DummyEvent> entry : fetchedData.entrySet()) {
      Assert.assertTrue(entry.getValue() == null);
      Assert.assertTrue(data.get(0).containsKey(entry.getKey()));
    }

    fetchedData = bucketStore.fetchBucket(1);
    for (Map.Entry<Object, DummyEvent> entry : fetchedData.entrySet()) {
      Assert.assertTrue(entry.getValue() == null);
      Assert.assertTrue(data.get(1).containsKey(entry.getKey()));
    }
  }

  void testDeleteBucket() throws Exception
  {
    bucketStore.storeBucketData(0, 0, data);
    bucketStore.deleteBucket(1);
    Map<Object, DummyEvent> fetchedData = bucketStore.fetchBucket(1);
    Assert.assertNotNull(fetchedData);
    Assert.assertTrue(fetchedData.size() == 0);
    deleteFsPath(rootBucketPath);
  }

  void deleteFsPath(Path path) throws IOException
  {
    fs.delete(path, true);
  }

  boolean bucketExists(int fileId)
  {
    Path bucketPath = new Path(applicationPath + HdfsBucketStore.PATH_SEPARATOR + 7 + HdfsBucketStore.PATH_SEPARATOR
      + fileId);
    try {
      return fs.exists(bucketPath);
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static final Logger logger = LoggerFactory.getLogger(BucketStoreTests.class);
}
