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

import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import org.apache.apex.malhar.lib.state.managed.ManagedStateTestUtils;
import org.apache.apex.malhar.lib.state.spillable.managed.ManagedStateSpillableStateStore;
import org.apache.apex.malhar.lib.utils.serde.CollectionSerde;
import org.apache.apex.malhar.lib.utils.serde.DefaultSerializationBuffer;
import org.apache.apex.malhar.lib.utils.serde.Serde;
import org.apache.apex.malhar.lib.utils.serde.SerializationBuffer;
import org.apache.apex.malhar.lib.utils.serde.SliceUtils;
import org.apache.apex.malhar.lib.utils.serde.StringSerde;
import org.apache.apex.malhar.lib.utils.serde.WindowedBlockStream;
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
  public static StringSerde SERDE_STRING_SLICE = new StringSerde();
  public static CollectionSerde<String, List<String>> SERDE_STRING_LIST_SLICE = new CollectionSerde<>(new StringSerde(),
      (Class)ArrayList.class);

  private SpillableTestUtils()
  {
    //Shouldn't instantiate this
  }

  public static class TestMeta extends TestWatcher
  {
    public ManagedStateSpillableStateStore store;
    public Context.OperatorContext operatorContext;
    public String applicationPath;

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

  protected static SerializationBuffer buffer = new DefaultSerializationBuffer(new WindowedBlockStream());

  public static Slice getKeySlice(byte[] id, String key)
  {
    SERDE_STRING_SLICE.serialize(key, buffer);
    return SliceUtils.concatenate(id, buffer.toSlice());
  }

  public static Slice getKeySlice(byte[] id, int index, String key)
  {
    SERDE_STRING_SLICE.serialize(key, buffer);
    return SliceUtils.concatenate(id,
        SliceUtils.concatenate(GPOUtils.serializeInt(index),
            buffer.toSlice()));
  }

  public static void checkValue(SpillableStateStore store, long bucketId, String key,
      byte[] prefix, String expectedValue)
  {
    SERDE_STRING_SLICE.serialize(key, buffer);
    checkValue(store, bucketId, SliceUtils.concatenate(prefix, buffer.toSlice()).buffer,
        expectedValue, 0, SERDE_STRING_SLICE);
  }

  public static void checkValue(SpillableStateStore store, long bucketId,
      byte[] prefix, int index, List<String> expectedValue)
  {
    checkValue(store, bucketId, SliceUtils.concatenate(prefix, GPOUtils.serializeInt(index)), expectedValue, 0,
        SERDE_STRING_LIST_SLICE);
  }

  public static <T> void  checkValue(SpillableStateStore store, long bucketId, byte[] bytes,
      T expectedValue, int offset, Serde<T> serde)
  {
    Slice slice = store.getSync(bucketId, new Slice(bytes));

    if (slice == null || slice.length == 0) {
      if (expectedValue != null) {
        Assert.assertEquals(expectedValue, slice);
      } else {
        return;
      }
    }

    T string = serde.deserialize(slice.buffer, new MutableInt(slice.offset + offset), slice.length);

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
