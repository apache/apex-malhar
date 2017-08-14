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
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import org.joda.time.Duration;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import org.apache.apex.malhar.lib.fileaccess.FileAccessFSImpl;
import org.apache.apex.malhar.lib.util.TestUtils;

import com.google.common.collect.Sets;

import com.datatorrent.api.Context;
import com.datatorrent.netlet.util.Slice;

public class StateTrackerTest
{
  static class TestMeta extends TestWatcher
  {
    MockManagedStateImpl managedState;
    Context.OperatorContext operatorContext;
    String applicationPath;

    @Override
    protected void starting(Description description)
    {
      TestUtils.deleteTargetTestClassFolder(description);
      managedState = new MockManagedStateImpl();
      applicationPath = "target/" + description.getClassName() + "/" + description.getMethodName();
      ((FileAccessFSImpl)managedState.getFileAccess()).setBasePath(applicationPath + "/" + "bucket_data");

      managedState.setNumBuckets(2);
      managedState.setMaxMemorySize(100);

      operatorContext = ManagedStateTestUtils.getOperatorContext(1, applicationPath);
    }

    @Override
    protected void finished(Description description)
    {
      TestUtils.deleteTargetTestClassFolder(description);
    }
  }

  @Rule
  public TestMeta testMeta = new TestMeta();

  @Test
  public void testEviction() throws InterruptedException
  {
    testMeta.managedState.latch = new CountDownLatch(1);
    testMeta.managedState.setup(testMeta.operatorContext);

    Slice one = ManagedStateTestUtils.getSliceFor("1");
    testMeta.managedState.beginWindow(System.currentTimeMillis());
    testMeta.managedState.put(1, one, one);
    testMeta.managedState.endWindow();

    testMeta.managedState.latch.await();
    testMeta.managedState.teardown();
    Assert.assertEquals("freed bucket", Sets.newHashSet(1L), testMeta.managedState.freedBuckets);
  }

  @Test
  public void testMultipleEvictions() throws InterruptedException
  {
    testMeta.managedState.latch = new CountDownLatch(2);
    testMeta.managedState.setup(testMeta.operatorContext);

    Slice one = ManagedStateTestUtils.getSliceFor("1");
    testMeta.managedState.beginWindow(System.currentTimeMillis());
    testMeta.managedState.put(1, one, one);

    Slice two = ManagedStateTestUtils.getSliceFor("2");
    testMeta.managedState.put(2, two, two);
    testMeta.managedState.endWindow();

    testMeta.managedState.latch.await();
    testMeta.managedState.teardown();
    Assert.assertEquals("freed bucket", Sets.newHashSet(1L, 2L), testMeta.managedState.freedBuckets);
  }

  @Test
  public void testBucketPrevention() throws InterruptedException
  {
    testMeta.managedState.setDurationPreventingFreeingSpace(Duration.standardDays(2));
    testMeta.managedState.setStateTracker(new MockStateTracker());
    testMeta.managedState.latch = new CountDownLatch(1);

    testMeta.managedState.setup(testMeta.operatorContext);
    Slice one = ManagedStateTestUtils.getSliceFor("1");
    testMeta.managedState.beginWindow(System.currentTimeMillis());
    testMeta.managedState.put(1, one, one);

    Slice two = ManagedStateTestUtils.getSliceFor("2");
    testMeta.managedState.put(2, two, two);
    testMeta.managedState.endWindow();

    testMeta.managedState.latch.await();
    testMeta.managedState.teardown();
    Assert.assertEquals("no buckets triggered", 0, testMeta.managedState.freedBuckets.size());
  }

  private static class MockManagedStateImpl extends ManagedStateImpl
  {
    CountDownLatch latch;
    Set<Long> freedBuckets = Sets.newHashSet();

    @Override
    protected Bucket newBucket(long bucketId)
    {
      return new MockDefaultBucket(bucketId);
    }
  }

  private static class MockDefaultBucket extends Bucket.DefaultBucket
  {

    protected MockDefaultBucket(long bucketId)
    {
      super(bucketId);
    }

    @Override
    public long freeMemory(long windowId) throws IOException
    {
      long freedBytes = super.freeMemory(windowId);
      if (((MockManagedStateImpl)managedStateContext).freedBuckets.add(getBucketId())) {
        ((MockManagedStateImpl)managedStateContext).latch.countDown();
      }
      return freedBytes;
    }

    @Override
    public long getSizeInBytes()
    {
      return 600;
    }
  }

  private static class MockStateTracker extends StateTracker
  {

    @Override
    public void run()
    {
      super.run();
      ((MockManagedStateImpl)managedStateImpl).latch.countDown();
    }
  }

}
