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

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import com.datatorrent.lib.fileaccess.FileAccess;
import com.datatorrent.lib.fileaccess.FileAccessFSImpl;
import com.datatorrent.netlet.util.Slice;

public class DefaultBucketTest
{

  class TestMeta extends TestWatcher
  {
    Bucket.DefaultBucket defaultBucket;
    String applicationPath;
    MockManagedStateContext managedStateContext;

    @Override
    protected void starting(Description description)
    {
      ManagedStateTestUtils.cleanTargetDir(description);
      managedStateContext = new MockManagedStateContext(ManagedStateTestUtils.getOperatorContext(9));
      applicationPath = "target/" + description.getClassName() + "/" + description.getMethodName();
      ((FileAccessFSImpl)managedStateContext.getFileAccess()).setBasePath(applicationPath + "/" + "bucket_data");
      managedStateContext.getFileAccess().init();

      defaultBucket = new Bucket.DefaultBucket(1);
      managedStateContext.getBucketsFileSystem().setup(managedStateContext);
    }

    @Override
    protected void finished(Description description)
    {
      managedStateContext.getBucketsFileSystem().teardown();
      ManagedStateTestUtils.cleanTargetDir(description);
    }
  }

  @Rule
  public TestMeta testMeta = new TestMeta();

  @Test
  public void testPut()
  {
    testMeta.defaultBucket.setup(testMeta.managedStateContext);
    Slice one = ManagedStateTestUtils.getSliceFor("1");
    testMeta.defaultBucket.put(one, 1, one);

    Slice value = testMeta.defaultBucket.get(one, 1, Bucket.ReadSource.MEMORY);
    Assert.assertEquals("value one", one, value);

    value = testMeta.defaultBucket.get(one, 1, Bucket.ReadSource.READERS);
    Assert.assertNull("value not present", value);

    Assert.assertEquals("size of bucket", one.length * 2 + 64, testMeta.defaultBucket.getSizeInBytes());
    testMeta.defaultBucket.teardown();
  }

  @Test
  public void testGetFromReader() throws IOException
  {
    testMeta.defaultBucket.setup(testMeta.managedStateContext);
    Slice one = ManagedStateTestUtils.getSliceFor("1");

    Map<Slice, Bucket.BucketedValue> unsavedBucket0 = ManagedStateTestUtils.getTestBucketData(0, 100);
    testMeta.managedStateContext.getBucketsFileSystem().writeBucketData(1, 1, unsavedBucket0);

    ManagedStateTestUtils.transferBucketHelper(testMeta.managedStateContext.getFileAccess(), 1, unsavedBucket0, 1);

    Slice value = testMeta.defaultBucket.get(one, -1, Bucket.ReadSource.READERS);
    Assert.assertEquals("value one", one, value);

    testMeta.defaultBucket.teardown();
  }

  @Test
  public void testGetFromSpecificTimeBucket() throws IOException
  {
    testMeta.defaultBucket.setup(testMeta.managedStateContext);
    Slice one = ManagedStateTestUtils.getSliceFor("1");

    Map<Slice, Bucket.BucketedValue> unsavedBucket0 = ManagedStateTestUtils.getTestBucketData(0, 100);
    testMeta.managedStateContext.getBucketsFileSystem().writeBucketData(1, 1, unsavedBucket0);

    ManagedStateTestUtils.transferBucketHelper(testMeta.managedStateContext.getFileAccess(), 1, unsavedBucket0, 1);

    Slice value = testMeta.defaultBucket.get(one, 101, Bucket.ReadSource.READERS);
    Assert.assertEquals("value one", one, value);

    testMeta.defaultBucket.teardown();
  }

  @Test
  public void testCheckpointed()
  {
    testMeta.defaultBucket.setup(testMeta.managedStateContext);
    Slice one = ManagedStateTestUtils.getSliceFor("1");
    testPut();
    Map<Slice, Bucket.BucketedValue> unsaved = testMeta.defaultBucket.checkpoint(10);
    Assert.assertEquals("size", 1, unsaved.size());

    Map.Entry<Slice, Bucket.BucketedValue> entry = unsaved.entrySet().iterator().next();
    Assert.assertEquals("key", one, entry.getKey());
    Assert.assertEquals("value", one, entry.getValue().getValue());
    Assert.assertEquals("time bucket", 1, entry.getValue().getTimeBucket());
    testMeta.defaultBucket.teardown();
  }

  @Test
  public void testCommitted()
  {
    testMeta.defaultBucket.setup(testMeta.managedStateContext);
    Slice one = ManagedStateTestUtils.getSliceFor("1");
    testCheckpointed();
    testMeta.defaultBucket.committed(10);
    Slice value = testMeta.defaultBucket.get(one, -1, Bucket.ReadSource.MEMORY);
    Assert.assertEquals("value one", one, value);
    testMeta.defaultBucket.teardown();
  }

  @Test
  public void testCommittedWithOpenReader() throws IOException
  {
    testMeta.defaultBucket.setup(testMeta.managedStateContext);
    testGetFromReader();
    Map<Long, FileAccess.FileReader> readers = testMeta.defaultBucket.getReaders();
    Assert.assertTrue("reader open", readers.containsKey(101L));

    Slice two = ManagedStateTestUtils.getSliceFor("2");
    Slice one = ManagedStateTestUtils.getSliceFor("1");

    testMeta.defaultBucket.put(two, 101, two);
    Map<Slice, Bucket.BucketedValue> unsaved = testMeta.defaultBucket.checkpoint(10);
    Assert.assertEquals("size", 1, unsaved.size());
    testMeta.defaultBucket.committed(10);

    Slice value = testMeta.defaultBucket.get(two, -1, Bucket.ReadSource.MEMORY);
    Assert.assertEquals("value two", two, value);

    value = testMeta.defaultBucket.get(one, -1, Bucket.ReadSource.MEMORY);
    Assert.assertEquals("value one", one, value);

    Assert.assertTrue("reader closed", !readers.containsKey(101L));
    testMeta.defaultBucket.teardown();
  }

  @Test
  public void testTeardown() throws IOException
  {
    testMeta.defaultBucket.setup(testMeta.managedStateContext);
    testGetFromReader();
    Map<Long, FileAccess.FileReader> readers = testMeta.defaultBucket.getReaders();
    Assert.assertTrue("reader open", readers.containsKey(101L));

    testMeta.defaultBucket.teardown();
    Assert.assertTrue("reader closed", readers.containsKey(101L));
  }

  @Test
  public void testFreeMemory() throws IOException
  {
    testMeta.defaultBucket.setup(testMeta.managedStateContext);
    testGetFromReader();
    long initSize = testMeta.defaultBucket.getSizeInBytes();

    Slice two = ManagedStateTestUtils.getSliceFor("2");
    testMeta.defaultBucket.put(two, 101, two);

    Assert.assertEquals("size", initSize + (two.length * 2 + 64), testMeta.defaultBucket.getSizeInBytes());

    long sizeFreed = testMeta.defaultBucket.freeMemory();
    Assert.assertEquals("size freed", initSize, sizeFreed);
    Assert.assertEquals("existing size", (two.length * 2 + 64), testMeta.defaultBucket.getSizeInBytes());
    testMeta.defaultBucket.teardown();
  }

}
