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
import org.apache.apex.malhar.lib.util.KryoCloneUtils;
import org.apache.apex.malhar.lib.util.TestUtils;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.Context;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG;
import com.datatorrent.netlet.util.Slice;

import static org.apache.apex.malhar.lib.helper.OperatorContextTestHelper.mockOperatorContext;

public class ManagedTimeStateImplTest
{
  class TestMeta extends TestWatcher
  {
    ManagedTimeStateImpl managedState;
    Context.OperatorContext operatorContext;
    String applicationPath;

    @Override
    protected void starting(Description description)
    {
      TestUtils.deleteTargetTestClassFolder(description);
      managedState = new ManagedTimeStateImpl();
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
    ManagedTimeStateImpl deserialized = KryoCloneUtils.cloneObject(testMeta.managedState);
    Assert.assertEquals("num buckets", deserialized.getNumBuckets(), testMeta.managedState.getNumBuckets());
  }

  @Test
  public void testAsyncGetFromReaders() throws IOException, ExecutionException, InterruptedException
  {
    Slice zero = ManagedStateTestUtils.getSliceFor("0");
    long time = System.currentTimeMillis();

    DefaultBucket.setDisableBloomFilterByDefault(true);
    testMeta.managedState.setup(testMeta.operatorContext);

    Map<Slice, Bucket.BucketedValue> unsavedBucket0 = ManagedStateTestUtils.getTestBucketData(0, time);
    testMeta.managedState.bucketsFileSystem.writeBucketData(time, 0, unsavedBucket0, -1);
    ManagedStateTestUtils.validateBucketOnFileSystem(testMeta.managedState.getFileAccess(), 0, unsavedBucket0, 1);

    Future<Slice> valFuture = testMeta.managedState.getAsync(0, zero);

    Assert.assertEquals("value of zero", zero, valFuture.get());
    testMeta.managedState.teardown();
  }

  @Test
  public void testPutGetWithTime()
  {
    Slice one = ManagedStateTestUtils.getSliceFor("1");
    testMeta.managedState.setup(testMeta.operatorContext);
    long time = System.currentTimeMillis();
    testMeta.managedState.beginWindow(0);
    testMeta.managedState.put(0, time, one, one);
    Slice value = testMeta.managedState.getSync(0, time, one);
    testMeta.managedState.endWindow();

    Assert.assertEquals("value of one", one, value);
    testMeta.managedState.teardown();
  }

  @Test
  public void testAsyncGetWithTime() throws ExecutionException, InterruptedException
  {
    Slice one = ManagedStateTestUtils.getSliceFor("1");
    testMeta.managedState.setup(testMeta.operatorContext);
    long time = System.currentTimeMillis();
    testMeta.managedState.beginWindow(0);
    testMeta.managedState.put(0, time, one, one);
    Future<Slice> valFuture = testMeta.managedState.getAsync(0, time, one);
    Slice value = valFuture.get();

    Assert.assertEquals("value of one", one, value);
    testMeta.managedState.teardown();
  }

  @Test
  public void testRecovery() throws ExecutionException, InterruptedException
  {
    Slice one = ManagedStateTestUtils.getSliceFor("1");
    testMeta.managedState.setup(testMeta.operatorContext);
    long time = System.currentTimeMillis();
    testMeta.managedState.beginWindow(0);
    testMeta.managedState.put(0, time, one, one);
    testMeta.managedState.endWindow();
    testMeta.managedState.beforeCheckpoint(0);

    testMeta.managedState.teardown();

    //there is a failure and the operator is re-deployed.
    testMeta.managedState.setStateTracker(new StateTracker());

    Attribute.AttributeMap.DefaultAttributeMap attributes = new Attribute.AttributeMap.DefaultAttributeMap();
    attributes.put(DAG.APPLICATION_PATH, testMeta.applicationPath);
    attributes.put(Context.OperatorContext.ACTIVATION_WINDOW_ID, 0L);
    OperatorContext operatorContext = mockOperatorContext(1, attributes);

    testMeta.managedState.setup(operatorContext);

    Bucket.DefaultBucket defaultBucket = (Bucket.DefaultBucket)testMeta.managedState.getBucket(0);
    Assert.assertEquals("value of one", one, defaultBucket.get(one, time, Bucket.ReadSource.MEMORY));
  }
}
