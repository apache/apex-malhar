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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.joda.time.Duration;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.fileaccess.FileAccessFSImpl;
import org.apache.apex.malhar.lib.util.KryoCloneUtils;
import org.apache.apex.malhar.lib.util.TestUtils;

import com.datatorrent.api.Context;
import com.datatorrent.netlet.util.Slice;

public class ManagedStateImplTest
{

  class TestMeta extends TestWatcher
  {
    ManagedStateImpl managedState;
    Context.OperatorContext operatorContext;
    String applicationPath;

    @Override
    protected void starting(Description description)
    {
      TestUtils.deleteTargetTestClassFolder(description);
      managedState = new ManagedStateImpl();
      applicationPath = "target/" + description.getClassName() + "/" + description.getMethodName();
      ((FileAccessFSImpl)managedState.getFileAccess()).setBasePath(applicationPath + "/" + "bucket_data");

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
  public void testSerde() throws IOException
  {
    ManagedStateImpl deserialized = KryoCloneUtils.cloneObject(testMeta.managedState);
    Assert.assertEquals("num buckets", deserialized.getNumBuckets(), testMeta.managedState.getNumBuckets());
  }

  @Test
  public void testSimplePutGet()
  {
    Slice one = ManagedStateTestUtils.getSliceFor("1");
    testMeta.managedState.setup(testMeta.operatorContext);
    testMeta.managedState.beginWindow(System.currentTimeMillis());
    testMeta.managedState.put(0, one, one);
    Slice value = testMeta.managedState.getSync(0, one);
    testMeta.managedState.endWindow();

    Assert.assertEquals("value of one", one, value);
    testMeta.managedState.teardown();
  }

  @Test
  public void testAsyncGetFromFlash() throws ExecutionException, InterruptedException
  {
    Slice one = ManagedStateTestUtils.getSliceFor("1");
    testMeta.managedState.setup(testMeta.operatorContext);
    testMeta.managedState.beginWindow(System.currentTimeMillis());
    testMeta.managedState.put(0, one, one);
    Future<Slice> valFuture = testMeta.managedState.getAsync(0, one);
    Slice value = valFuture.get();

    Assert.assertEquals("value of one", one, value);
    testMeta.managedState.teardown();
  }

  @Test
  public void testIncrementalCheckpoint()
  {
    Slice one = ManagedStateTestUtils.getSliceFor("1");
    testMeta.managedState.setup(testMeta.operatorContext);
    long time = System.currentTimeMillis();
    testMeta.managedState.beginWindow(time);
    testMeta.managedState.put(0, one, one);
    testMeta.managedState.endWindow();
    testMeta.managedState.beforeCheckpoint(time);

    Bucket.DefaultBucket defaultBucket = (Bucket.DefaultBucket)testMeta.managedState.getBucket(0);
    Assert.assertEquals("value of one", one, defaultBucket.getCheckpointedData().get(time).get(one).getValue());

    Slice two = ManagedStateTestUtils.getSliceFor("2");
    testMeta.managedState.beginWindow(time + 1);
    testMeta.managedState.put(0, two, two);
    testMeta.managedState.endWindow();
    testMeta.managedState.beforeCheckpoint(time + 1);

    Assert.assertEquals("value of two", two, defaultBucket.getCheckpointedData().get(time + 1).get(two).getValue());
    testMeta.managedState.teardown();
  }

  @Test
  public void testAsyncGetFromCheckpoint() throws ExecutionException, InterruptedException
  {
    Slice one = ManagedStateTestUtils.getSliceFor("1");
    testMeta.managedState.setup(testMeta.operatorContext);
    long time = System.currentTimeMillis();
    testMeta.managedState.beginWindow(time);
    testMeta.managedState.put(0, one, one);
    testMeta.managedState.endWindow();
    testMeta.managedState.beforeCheckpoint(time);

    Future<Slice> valFuture = testMeta.managedState.getAsync(0, one);
    Assert.assertEquals("value of one", one, valFuture.get());
    testMeta.managedState.teardown();
  }

  @Test
  public void testCommitted()
  {
    Slice one = ManagedStateTestUtils.getSliceFor("1");
    Slice two = ManagedStateTestUtils.getSliceFor("2");
    commitHelper(one, two);
    Bucket.DefaultBucket defaultBucket = (Bucket.DefaultBucket)testMeta.managedState.getBucket(0);
    Assert.assertEquals("value of one", one, defaultBucket.getCommittedData().firstEntry().getValue().get(one)
        .getValue());
    Assert.assertNull("value of two", defaultBucket.getCommittedData().firstEntry().getValue().get(two));
    testMeta.managedState.teardown();
  }

  @Test
  public void testAsyncGetFromCommitted() throws ExecutionException, InterruptedException
  {
    Slice one = ManagedStateTestUtils.getSliceFor("1");
    Slice two = ManagedStateTestUtils.getSliceFor("2");
    commitHelper(one, two);
    Future<Slice> valFuture = testMeta.managedState.getAsync(0, one);
    Assert.assertEquals("value of one", one, valFuture.get());
  }

  /**
   * Fix for APEXMALHAR-2042
   */
  @Test
  public void testFreeWindowTransferRaceCondition() throws Exception
  {
    testMeta.managedState.setMaxMemorySize(1);
    testMeta.managedState.setCheckStateSizeInterval(Duration.millis(1L));
    testMeta.managedState.setup(testMeta.operatorContext);

    int numKeys = 300;
    long lastWindowId = numKeys;

    for (long windowId = 0L; windowId < lastWindowId; windowId++) {
      testMeta.managedState.beginWindow(windowId);
      Slice keyVal = ManagedStateTestUtils.getSliceFor(Long.toString(windowId));
      testMeta.managedState.put(0L, keyVal, keyVal);
      testMeta.managedState.endWindow();
      testMeta.managedState.beforeCheckpoint(windowId);
      testMeta.managedState.checkpointed(windowId);
      Thread.sleep(1L);
    }

    testMeta.managedState.committed(lastWindowId - 2L);
    lastWindowId++;

    testMeta.managedState.beginWindow(lastWindowId);

    for (int key = numKeys - 1; key > 0; key--) {
      Slice keyVal = ManagedStateTestUtils.getSliceFor(Integer.toString(key));
      Slice val = testMeta.managedState.getSync(0L, keyVal);
      Assert.assertNotNull("null value for key " + key, val);
    }

    testMeta.managedState.endWindow();
    testMeta.managedState.teardown();
  }

  private void commitHelper(Slice one, Slice two)
  {
    testMeta.managedState.setup(testMeta.operatorContext);
    long time = System.currentTimeMillis();
    testMeta.managedState.beginWindow(time);
    testMeta.managedState.put(0, one, one);
    testMeta.managedState.endWindow();
    testMeta.managedState.beforeCheckpoint(time);

    testMeta.managedState.beginWindow(time + 1);
    testMeta.managedState.put(0, two, two);
    testMeta.managedState.endWindow();
    testMeta.managedState.beforeCheckpoint(time + 1);

    testMeta.managedState.committed(time);
  }

  private static final Logger LOG = LoggerFactory.getLogger(ManagedStateImplTest.class);
}
