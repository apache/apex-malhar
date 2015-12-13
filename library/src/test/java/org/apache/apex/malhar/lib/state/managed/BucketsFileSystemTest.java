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

package org.apache.apex.malhar.lib.state.managed;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeSet;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import com.datatorrent.lib.fileaccess.FileAccessFSImpl;
import com.datatorrent.netlet.util.Slice;

public class BucketsFileSystemTest
{
  class TestMeta extends TestWatcher
  {
    BucketsFileSystem bucketsFileSystem;
    String applicationPath;
    MockManagedStateContext managedStateContext;

    @Override
    protected void starting(Description description)
    {
      ManagedStateTestUtils.cleanTargetDir(description);

      managedStateContext = new MockManagedStateContext(ManagedStateTestUtils.getOperatorContext(7));
      applicationPath = "target/" + description.getClassName() + "/" + description.getMethodName();
      ((FileAccessFSImpl)managedStateContext.getFileAccess()).setBasePath(applicationPath + "/" + "bucket_data");
      managedStateContext.getFileAccess().init();

      bucketsFileSystem = new BucketsFileSystem();

    }

    @Override
    protected void finished(Description description)
    {
      ManagedStateTestUtils.cleanTargetDir(description);
    }

  }

  @Rule
  public TestMeta testMeta = new TestMeta();

  @Test
  public void testTransferBucket() throws IOException
  {
    testMeta.bucketsFileSystem.setup(testMeta.managedStateContext);
    Map<Slice, Bucket.BucketedValue> unsavedBucket0 = ManagedStateTestUtils.getTestBucketData(0, 100);
    testMeta.bucketsFileSystem.writeBucketData(10, 0, unsavedBucket0);

    ManagedStateTestUtils.transferBucketHelper(testMeta.managedStateContext.getFileAccess(), 0, unsavedBucket0, 1);
    testMeta.bucketsFileSystem.teardown();
  }

  @Test
  public void testTransferOfExistingBucket() throws IOException
  {
    testMeta.bucketsFileSystem.setup(testMeta.managedStateContext);
    Map<Slice, Bucket.BucketedValue> unsavedBucket0 = ManagedStateTestUtils.getTestBucketData(0, 100);
    testMeta.bucketsFileSystem.writeBucketData(10, 0, unsavedBucket0);

    Map<Slice, Bucket.BucketedValue> more = ManagedStateTestUtils.getTestBucketData(50, 100);
    testMeta.bucketsFileSystem.writeBucketData(10, 0, more);

    unsavedBucket0.putAll(more);
    ManagedStateTestUtils.transferBucketHelper(testMeta.managedStateContext.getFileAccess(), 0, unsavedBucket0, 2);
    testMeta.bucketsFileSystem.teardown();
  }

  @Test
  public void testUpdateBucketMetaDataFile() throws IOException
  {
    testMeta.bucketsFileSystem.setup(testMeta.managedStateContext);
    BucketsFileSystem.MutableTimeBucketMeta mutableTbm = testMeta.bucketsFileSystem.getOrCreateTimeBucketMeta(1, 1);
    mutableTbm.updateTimeBucketMeta(10, 100, new Slice("1".getBytes()));

    testMeta.bucketsFileSystem.updateBucketMetaFile(1);
    BucketsFileSystem.TimeBucketMeta immutableTbm = testMeta.bucketsFileSystem.getTimeBucketMeta(1, 1);
    Assert.assertNotNull(immutableTbm);
    Assert.assertEquals("last transferred window", 10, immutableTbm.getLastTransferredWindowId());
    Assert.assertEquals("size in bytes", 100, immutableTbm.getSizeInBytes());
    Assert.assertEquals("first key", "1", immutableTbm.getFirstKey().stringValue());
    testMeta.bucketsFileSystem.teardown();
  }

  @Test
  public void testGetTimeBucketMeta() throws IOException
  {
    testMeta.bucketsFileSystem.setup(testMeta.managedStateContext);
    BucketsFileSystem.TimeBucketMeta bucketMeta = testMeta.bucketsFileSystem.getTimeBucketMeta(1, 1);
    Assert.assertNull("bucket meta", bucketMeta);

    testMeta.bucketsFileSystem.getOrCreateTimeBucketMeta(1, 1);
    bucketMeta = testMeta.bucketsFileSystem.getTimeBucketMeta(1, 1);
    Assert.assertNotNull("bucket meta not null", bucketMeta);
    testMeta.bucketsFileSystem.teardown();
  }

  @Test
  public void testGetAllTimeBucketMeta() throws IOException
  {
    testMeta.bucketsFileSystem.setup(testMeta.managedStateContext);
    BucketsFileSystem.MutableTimeBucketMeta tbm1 = testMeta.bucketsFileSystem.getOrCreateTimeBucketMeta(1, 1);
    tbm1.updateTimeBucketMeta(10, 100, new Slice("1".getBytes()));

    BucketsFileSystem.MutableTimeBucketMeta tbm2 = testMeta.bucketsFileSystem.getOrCreateTimeBucketMeta(1, 2);
    tbm2.updateTimeBucketMeta(10, 100, new Slice("2".getBytes()));

    testMeta.bucketsFileSystem.updateBucketMetaFile(1);
    TreeSet<BucketsFileSystem.TimeBucketMeta> timeBucketMetas =
        testMeta.bucketsFileSystem.getAllTimeBuckets(1);

    Iterator<BucketsFileSystem.TimeBucketMeta> iterator = timeBucketMetas.iterator();
    int i = 2;
    while (iterator.hasNext()) {
      BucketsFileSystem.TimeBucketMeta tbm = iterator.next();
      Assert.assertEquals("time bucket " + i, i, tbm.getTimeBucketId());
      i--;
    }
    testMeta.bucketsFileSystem.teardown();
  }

  @Test
  public void testInvalidateTimeBucket() throws IOException
  {
    testMeta.bucketsFileSystem.setup(testMeta.managedStateContext);
    testGetAllTimeBucketMeta();
    testMeta.bucketsFileSystem.invalidateTimeBucket(1, 1);
    BucketsFileSystem.TimeBucketMeta immutableTbm = testMeta.bucketsFileSystem.getTimeBucketMeta(1,1);
    Assert.assertNull("deleted tbm", immutableTbm);

    TreeSet<BucketsFileSystem.TimeBucketMeta> timeBucketMetas =
        testMeta.bucketsFileSystem.getAllTimeBuckets(1);

    Assert.assertEquals("only 1 tbm", 1, timeBucketMetas.size());
    immutableTbm = timeBucketMetas.iterator().next();

    Assert.assertEquals("tbm 2", 2, immutableTbm.getTimeBucketId());
    testMeta.bucketsFileSystem.teardown();
  }
}
