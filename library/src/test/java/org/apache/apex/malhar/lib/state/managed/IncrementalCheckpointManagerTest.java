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
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import javax.validation.constraints.NotNull;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import com.google.common.base.Preconditions;

import com.datatorrent.api.Context;
import com.datatorrent.lib.fileaccess.FileAccessFSImpl;
import com.datatorrent.lib.util.KryoCloneUtils;
import com.datatorrent.lib.util.TestUtils;
import com.datatorrent.netlet.util.Slice;

public class IncrementalCheckpointManagerTest
{
  class TestMeta extends TestWatcher
  {
    IncrementalCheckpointManager checkpointManager;
    String applicationPath;
    int operatorId = 1;
    MockManagedStateContext managedStateContext;

    @Override
    protected void starting(Description description)
    {
      TestUtils.deleteTargetTestClassFolder(description);
      applicationPath = "target/" + description.getClassName() + "/" + description.getMethodName();

      Context.OperatorContext operatorContext = ManagedStateTestUtils.getOperatorContext(operatorId, applicationPath);
      managedStateContext = new MockManagedStateContext(operatorContext);

      ((FileAccessFSImpl)managedStateContext.getFileAccess()).setBasePath(applicationPath + "/" + "bucket_data");
      managedStateContext.getFileAccess().init();

      checkpointManager = new IncrementalCheckpointManager();

      managedStateContext.getTimeBucketAssigner().setup(managedStateContext);
      managedStateContext.getBucketsFileSystem().setup(managedStateContext);
    }

    @Override
    protected void finished(Description description)
    {
      managedStateContext.getTimeBucketAssigner().teardown();
      managedStateContext.getBucketsFileSystem().teardown();
      TestUtils.deleteTargetTestClassFolder(description);
    }
  }

  @Rule
  public TestMeta testMeta = new TestMeta();

  @Test
  public void testSerde() throws IOException
  {
    IncrementalCheckpointManager deserialized = KryoCloneUtils.cloneObject(testMeta.checkpointManager);
    Assert.assertNotNull("state window data manager", deserialized);
  }

  @Test
  public void testSave() throws IOException
  {
    testMeta.checkpointManager.setup(testMeta.managedStateContext);
    Map<Long, Map<Slice, Bucket.BucketedValue>> buckets5 = ManagedStateTestUtils.getTestData(0, 5, 0);
    testMeta.checkpointManager.save(buckets5, testMeta.operatorId, 10, false);
    testMeta.checkpointManager.teardown();

    testMeta.checkpointManager = new IncrementalCheckpointManager();
    testMeta.checkpointManager.setup(testMeta.managedStateContext);
    @SuppressWarnings("unchecked")
    Map<Long, Map<Slice, Bucket.BucketedValue>> buckets5After = (Map<Long, Map<Slice, Bucket.BucketedValue>>)
        testMeta.checkpointManager.load(testMeta.operatorId, 10);

    Assert.assertEquals("saved", buckets5, buckets5After);
    testMeta.checkpointManager.teardown();
  }

  @Test
  public void testTransferWindowFiles() throws IOException, InterruptedException
  {
    testMeta.checkpointManager.setup(testMeta.managedStateContext);

    Map<Long, Map<Slice, Bucket.BucketedValue>> buckets5 = ManagedStateTestUtils.getTestData(0, 5, 0);
    testMeta.checkpointManager.save(buckets5, testMeta.operatorId, 10, false);
    //Need to synchronously call transfer window files so shutting down the other thread.
    testMeta.checkpointManager.teardown();
    Thread.sleep(500);

    testMeta.checkpointManager.committed(testMeta.operatorId, 10);
    testMeta.checkpointManager.transferWindowFiles();

    for (int i = 0; i < 5; i++) {
      ManagedStateTestUtils.transferBucketHelper(testMeta.managedStateContext.getFileAccess(), i,
          buckets5.get((long)i), 1);
    }
  }

  @Test
  public void testCommitted() throws IOException, InterruptedException
  {
    CountDownLatch latch = new CountDownLatch(5);
    MockBucketsFileSystem mockBucketsFileSystem = new MockBucketsFileSystem(latch);

    testMeta.managedStateContext.setBucketsFileSystem(mockBucketsFileSystem);

    mockBucketsFileSystem.setup(testMeta.managedStateContext);
    testMeta.checkpointManager.setup(testMeta.managedStateContext);

    Map<Long, Map<Slice, Bucket.BucketedValue>> data = ManagedStateTestUtils.getTestData(0, 5, 0);
    testMeta.checkpointManager.save(data, testMeta.operatorId, 10, false);
    testMeta.checkpointManager.committed(testMeta.operatorId, 10);
    latch.await();
    testMeta.checkpointManager.teardown();
    Thread.sleep(500);

    for (int i = 0; i < 5; i++) {
      ManagedStateTestUtils.transferBucketHelper(testMeta.managedStateContext.getFileAccess(), i, data.get((long)i), 1);
    }
  }

  @Test
  public void testPurge() throws IOException, InterruptedException
  {
    FileSystem fileSystem = FileSystem.newInstance(new Configuration());

    testTransferWindowFiles();
    RemoteIterator<LocatedFileStatus> iterator = fileSystem.listLocatedStatus(
        new Path(testMeta.applicationPath + "/bucket_data"));
    Assert.assertTrue(iterator.hasNext());

    testMeta.managedStateContext.getBucketsFileSystem().deleteTimeBucketsLessThanEqualTo(200);

    iterator = fileSystem.listLocatedStatus(new Path(testMeta.applicationPath + "/bucket_data"));
    if (iterator.hasNext()) {
      Assert.fail("All buckets should be deleted");
    }
  }

  static class MockBucketsFileSystem extends BucketsFileSystem
  {
    private final transient CountDownLatch latch;

    public MockBucketsFileSystem(@NotNull CountDownLatch latch)
    {
      super();
      this.latch = Preconditions.checkNotNull(latch);
    }

    @Override
    protected void writeBucketData(long windowId, long bucketId, Map<Slice, Bucket.BucketedValue> data)
        throws IOException
    {
      super.writeBucketData(windowId, bucketId, data);
      if (windowId == 10) {
        latch.countDown();
      }
    }
  }

  private static final transient Logger LOG = LoggerFactory.getLogger(IncrementalCheckpointManagerTest.class);
}
