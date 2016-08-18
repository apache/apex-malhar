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
package org.apache.apex.malhar.lib.state.spillable;

import java.util.List;

import org.junit.Assert;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import org.apache.apex.malhar.lib.state.managed.ManagedStateTestUtils;
import org.apache.apex.malhar.lib.state.spillable.managed.ManagedStateSpillableStateStore;
import org.apache.apex.malhar.lib.utils.serde.Serde;
import org.apache.apex.malhar.lib.utils.serde.SerdeListSlice;
import org.apache.apex.malhar.lib.utils.serde.SerdeStringSlice;
import org.apache.apex.malhar.lib.utils.serde.SliceUtils;
import org.apache.commons.lang3.mutable.MutableInt;

import com.datatorrent.api.Context;
import com.datatorrent.lib.appdata.gpo.GPOUtils;
import com.datatorrent.lib.fileaccess.FileAccessFSImpl;
import com.datatorrent.lib.util.TestUtils;
import com.datatorrent.netlet.util.Slice;

/**
 * This class contains utility methods that can be used by Spillable data structure unit tests.
 */
public class SpillableTestUtils
{
  public static SerdeStringSlice SERDE_STRING_SLICE = new SerdeStringSlice();
  public static SerdeListSlice<String> SERDE_STRING_LIST_SLICE = new SerdeListSlice(new SerdeStringSlice());

  private SpillableTestUtils()
  {
    //Shouldn't instantiate this
  }

  public static class TestMeta extends TestWatcher
  {
    ManagedStateSpillableStateStore store;
    public Context.OperatorContext operatorContext;
    String applicationPath;

    @Override
    protected void starting(Description description)
    {
      TestUtils.deleteTargetTestClassFolder(description);
      store = new ManagedStateSpillableStateStore();
      applicationPath = "target/" + description.getClassName() + "/" + description.getMethodName();
      ((FileAccessFSImpl)store.getFileAccess()).setBasePath(applicationPath + "/" + "bucket_data");

      operatorContext = ManagedStateTestUtils.getOperatorContext(1, applicationPath);
    }

    @Override
    protected void finished(Description description)
    {
      TestUtils.deleteTargetTestClassFolder(description);
    }
  }

  public static Slice getKeySlice(byte[] id, String key)
  {
    return SliceUtils.concatenate(id, SERDE_STRING_SLICE.serialize(key));
  }

  public static Slice getKeySlice(byte[] id, int index, String key)
  {
    return SliceUtils.concatenate(id,
        SliceUtils.concatenate(GPOUtils.serializeInt(index),
            SERDE_STRING_SLICE.serialize(key)));
  }

  public static void checkValue(SpillableStateStore store, long bucketId, String key,
      byte[] prefix, String expectedValue)
  {
    checkValue(store, bucketId, SliceUtils.concatenate(prefix, SERDE_STRING_SLICE.serialize(key)).buffer,
        expectedValue, 0, SERDE_STRING_SLICE);
  }

  public static void checkValue(SpillableStateStore store, long bucketId,
      byte[] prefix, int index, List<String> expectedValue)
  {
    checkValue(store, bucketId, SliceUtils.concatenate(prefix, GPOUtils.serializeInt(index)), expectedValue, 0,
        SERDE_STRING_LIST_SLICE);
  }

  public static <T> void  checkValue(SpillableStateStore store, long bucketId, byte[] bytes,
      T expectedValue, int offset, Serde<T, Slice> serde)
  {
    Slice slice = store.getSync(bucketId, new Slice(bytes));

    if (slice == null || slice.length == 0) {
      if (expectedValue != null) {
        Assert.assertEquals(expectedValue, slice);
      } else {
        return;
      }
    }

    T string = serde.deserialize(slice, new MutableInt(offset));

    Assert.assertEquals(expectedValue, string);
  }

  public static void checkOutOfBounds(SpillableArrayListImpl<String> list, int index)
  {
    boolean exceptionThrown = false;

    try {
      list.get(index);
    } catch (IndexOutOfBoundsException ex) {
      exceptionThrown = true;
    }

    Assert.assertTrue(exceptionThrown);
  }
}
