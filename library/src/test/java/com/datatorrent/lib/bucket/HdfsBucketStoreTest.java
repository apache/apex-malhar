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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import com.datatorrent.lib.helper.OperatorContextTestHelper;

/**
 * Tests of {@link HdfsBucketStore}
 */
public class HdfsBucketStoreTest
{
  private static final String filepath = "src/test/resources/test_data/dt_spend";
  private static final String APPLICATION_PATH_PREFIX = "target/HdfsBucketStoreTest";
  private static final int TOTAL_BUCKETS = 1000;
  private static String applicationPath;
  private static Path rootBucketPath;
  private static HdfsBucketStore<DummyEvent> hdfsBucketStore;
  private static Map<Integer, Map<Object, DummyEvent>> data = Maps.newHashMap();

  @BeforeClass
  public static void setUp()
  {
    applicationPath = OperatorContextTestHelper.getUniqueApplicationPath(APPLICATION_PATH_PREFIX);
    hdfsBucketStore = new HdfsBucketStore<DummyEvent>();
    hdfsBucketStore.setNoOfBuckets(TOTAL_BUCKETS);
    hdfsBucketStore.setWriteEventKeysOnly(true);

    Map<String, Object> parameters = Maps.newHashMap();
    parameters.put(HdfsBucketStore.APP_PATH, applicationPath);
    parameters.put(HdfsBucketStore.OPERATOR_ID, 7);
    parameters.put(HdfsBucketStore.PARTITION_KEYS, Sets.newHashSet(0));
    parameters.put(HdfsBucketStore.PARTITION_MASK, 0);

    hdfsBucketStore.setup(new Context(parameters));

    for (int bucketIdx = 0; bucketIdx < 2; bucketIdx++) {
      Map<Object, DummyEvent> bucketData = Maps.newHashMap();
      data.put(bucketIdx, bucketData);

      for (int i = 0; i < 10; i++) {
        DummyEvent event = new DummyEvent(i, System.currentTimeMillis());
        bucketData.put(event.getEventKey(), event);
      }
    }
    rootBucketPath = new Path(applicationPath + HdfsBucketStore.PATH_SEPARATOR + HdfsBucketStore.BUCKETS_SUBDIR
      + HdfsBucketStore.PATH_SEPARATOR + 7);
  }

  @AfterClass
  public static void tearDown() throws Exception
  {
    Path root = new Path(applicationPath);
    FileSystem fs = FileSystem.get(root.toUri(), new Configuration());
    fs.delete(root, true);
  }

  @Test
  public void testStoreBucketData() throws Exception
  {
    hdfsBucketStore.storeBucketData(0, data);
    FileSystem fileSystem = FileSystem.get(rootBucketPath.toUri(), new Configuration());
    Assert.assertTrue(fileSystem.exists(rootBucketPath));
    deleteFsPath(rootBucketPath);
  }

  @Test
  public void testFetchBucket() throws Exception
  {
    hdfsBucketStore.storeBucketData(0, data);
    Map<Object, DummyEvent> fetchedData = hdfsBucketStore.fetchBucket(0);

    for (Map.Entry<Object, DummyEvent> entry : fetchedData.entrySet()) {
      Assert.assertTrue(entry.getValue() == null);
      Assert.assertTrue(data.get(0).containsKey(entry.getKey()));
    }

    fetchedData = hdfsBucketStore.fetchBucket(1);
    for (Map.Entry<Object, DummyEvent> entry : fetchedData.entrySet()) {
      Assert.assertTrue(entry.getValue() == null);
      Assert.assertTrue(data.get(1).containsKey(entry.getKey()));
    }
  }

  @Test
  public void testDeleteBucket() throws Exception
  {
    hdfsBucketStore.storeBucketData(0, data);
    hdfsBucketStore.deleteBucket(1);
    Map<Object, DummyEvent> fetchedData = hdfsBucketStore.fetchBucket(1);
    Assert.assertNotNull(fetchedData);
    Assert.assertTrue(fetchedData.size() == 0);
    deleteFsPath(rootBucketPath);
  }

  private void deleteFsPath(Path path) throws IOException
  {
    FileSystem fileSystem = FileSystem.get(path.toUri(), new Configuration());
    fileSystem.delete(path, true);
  }

  private static final Logger logger = LoggerFactory.getLogger(HdfsBucketStore.class);

}
