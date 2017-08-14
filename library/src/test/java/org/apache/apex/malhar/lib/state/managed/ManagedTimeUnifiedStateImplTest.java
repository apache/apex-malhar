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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import org.apache.apex.malhar.lib.fileaccess.FileAccessFSImpl;
import org.apache.apex.malhar.lib.state.managed.Bucket.DefaultBucket;
import org.apache.apex.malhar.lib.util.TestUtils;

import com.datatorrent.api.Context;
import com.datatorrent.netlet.util.Slice;

public class ManagedTimeUnifiedStateImplTest
{
  class TestMeta extends TestWatcher
  {
    ManagedTimeUnifiedStateImpl managedState;
    Context.OperatorContext operatorContext;
    String applicationPath;

    @Override
    protected void starting(Description description)
    {
      TestUtils.deleteTargetTestClassFolder(description);
      managedState = new ManagedTimeUnifiedStateImpl();
      applicationPath = "target/" + description.getClassName() + "/" + description.getMethodName();
      ((FileAccessFSImpl)managedState.getFileAccess()).setBasePath(applicationPath + "/" + "bucket_data");

      operatorContext = ManagedStateTestUtils.getOperatorContext(9, applicationPath);
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
  public void testSimplePutGet()
  {
    Slice one = ManagedStateTestUtils.getSliceFor("1");
    testMeta.managedState.setup(testMeta.operatorContext);
    long time = System.currentTimeMillis();
    testMeta.managedState.beginWindow(0);
    testMeta.managedState.put(time, one, one);
    Slice value = testMeta.managedState.getSync(time, one);
    testMeta.managedState.endWindow();

    Assert.assertEquals("value of one", one, value);
    testMeta.managedState.teardown();
  }

  @Test
  public void testPutWithMultipleValuesForAKey()
  {
    Slice one = ManagedStateTestUtils.getSliceFor("1");

    testMeta.managedState.setup(testMeta.operatorContext);
    long time = System.currentTimeMillis();
    testMeta.managedState.beginWindow(0);
    testMeta.managedState.put(time, one, one);

    Slice two = ManagedStateTestUtils.getSliceFor("2");
    testMeta.managedState.put(time, one, two);
    Slice value = testMeta.managedState.getSync(time, one);
    testMeta.managedState.endWindow();

    Assert.assertEquals("value overwritten", two, value);
    testMeta.managedState.teardown();
  }

  @Test
  public void testAsyncGet() throws ExecutionException, InterruptedException
  {
    Slice one = ManagedStateTestUtils.getSliceFor("1");
    testMeta.managedState.setup(testMeta.operatorContext);
    long time = System.currentTimeMillis();
    testMeta.managedState.beginWindow(0);
    testMeta.managedState.put(time, one, one);
    Future<Slice> valFuture = testMeta.managedState.getAsync(time, one);
    Slice value = valFuture.get();

    Assert.assertEquals("value of one", one, value);
    testMeta.managedState.teardown();
  }

  @Test
  public void testSyncGetFromFiles() throws IOException, ExecutionException, InterruptedException
  {
    DefaultBucket.setDisableBloomFilterByDefault(true);

    Slice zero = ManagedStateTestUtils.getSliceFor("0");
    long time = System.currentTimeMillis();

    testMeta.managedState.setup(testMeta.operatorContext);

    long timeBucket = testMeta.managedState.getTimeBucketAssigner().getTimeBucket(time);
    Map<Slice, Bucket.BucketedValue> unsavedBucket0 = ManagedStateTestUtils.getTestBucketData(0, timeBucket);

    //write data to disk explicitly
    testMeta.managedState.bucketsFileSystem.writeBucketData(time, 0, unsavedBucket0, -1);
    ManagedStateTestUtils.validateBucketOnFileSystem(testMeta.managedState.getFileAccess(),
        testMeta.operatorContext.getId(), unsavedBucket0, 1);

    Slice value = testMeta.managedState.getSync(time, zero);

    Assert.assertEquals("value of zero", zero, value);
    testMeta.managedState.teardown();
  }

  @Test
  public void testAsyncSyncGetFromFiles() throws IOException, ExecutionException, InterruptedException
  {
    DefaultBucket.setDisableBloomFilterByDefault(true);

    Slice zero = ManagedStateTestUtils.getSliceFor("0");
    long time = System.currentTimeMillis();

    testMeta.managedState.setup(testMeta.operatorContext);

    long timeBucket = testMeta.managedState.getTimeBucketAssigner().getTimeBucket(time);
    Map<Slice, Bucket.BucketedValue> unsavedBucket0 = ManagedStateTestUtils.getTestBucketData(0, timeBucket);

    //write data to disk explicitly
    testMeta.managedState.bucketsFileSystem.writeBucketData(time, 0, unsavedBucket0, -1);
    ManagedStateTestUtils.validateBucketOnFileSystem(testMeta.managedState.getFileAccess(),
        testMeta.operatorContext.getId(), unsavedBucket0, 1);

    Future<Slice> valFuture = testMeta.managedState.getAsync(time, zero);

    Assert.assertEquals("value of zero", zero, valFuture.get());
    testMeta.managedState.teardown();
  }


}
