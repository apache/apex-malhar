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

import org.apache.apex.malhar.lib.fileaccess.FileAccessFSImpl;
import org.apache.apex.malhar.lib.state.managed.ManagedStateTestUtils;
import org.apache.apex.malhar.lib.state.spillable.managed.ManagedStateSpillableStateStore;
import org.apache.apex.malhar.lib.state.spillable.managed.ManagedTimeUnifiedStateSpillableStateStore;
import org.apache.apex.malhar.lib.util.TestUtils;
import org.apache.apex.malhar.lib.utils.serde.CollectionSerde;
import org.apache.apex.malhar.lib.utils.serde.Serde;
import org.apache.apex.malhar.lib.utils.serde.SerializationBuffer;
import org.apache.apex.malhar.lib.utils.serde.StringSerde;
import org.apache.apex.malhar.lib.utils.serde.WindowedBlockStream;

import com.esotericsoftware.kryo.io.Input;

import com.datatorrent.api.Context;
import com.datatorrent.netlet.util.Slice;

/**
 * This class contains utility methods that can be used by Spillable data structure unit tests.
 */
public class SpillableTestUtils
{
  public static StringSerde STRING_SERDE = new StringSerde();
  public static CollectionSerde<String, List<String>> STRING_LIST_SERDE = new CollectionSerde<>(new StringSerde(),
      (Class)ArrayList.class);

  private SpillableTestUtils()
  {
    //Shouldn't instantiate this
  }

  public static class TestMeta extends TestWatcher
  {
    public ManagedStateSpillableStateStore store;
    public ManagedTimeUnifiedStateSpillableStateStore timeStore;
    public Context.OperatorContext operatorContext;
    public String applicationPath;

    @Override
    protected void starting(Description description)
    {
      TestUtils.deleteTargetTestClassFolder(description);
      store = new ManagedStateSpillableStateStore();
      timeStore = new ManagedTimeUnifiedStateSpillableStateStore();
      applicationPath = "target/" + description.getClassName() + "/" + description.getMethodName();
      ((FileAccessFSImpl)store.getFileAccess()).setBasePath(applicationPath + "/" + "bucket_data");
      ((FileAccessFSImpl)timeStore.getFileAccess()).setBasePath(applicationPath + "/" + "time_bucket_data");

      operatorContext = ManagedStateTestUtils.getOperatorContext(1, applicationPath);
    }

    @Override
    protected void finished(Description description)
    {
      TestUtils.deleteTargetTestClassFolder(description);
    }
  }

  protected static SerializationBuffer buffer = new SerializationBuffer(new WindowedBlockStream());

  public static Slice getKeySlice(byte[] id, String key)
  {
    buffer.writeBytes(id);
    STRING_SERDE.serialize(key, buffer);
    return buffer.toSlice();
  }

  public static Slice getKeySlice(byte[] id, int index, String key)
  {
    buffer.writeBytes(id);
    buffer.writeInt(index);
    STRING_SERDE.serialize(key, buffer);
    return buffer.toSlice();
  }

  public static void checkValue(SpillableStateStore store, long bucketId, String key,
      byte[] prefix, String expectedValue)
  {
    buffer.writeBytes(prefix);
    STRING_SERDE.serialize(key, buffer);
    checkValue(store, bucketId, buffer.toSlice().toByteArray(), expectedValue, 0, STRING_SERDE);
  }

  public static void checkValue(SpillableStateStore store, long bucketId,
      byte[] prefix, int index, List<String> expectedValue)
  {
    buffer.writeBytes(prefix);
    buffer.writeInt(index);
    checkValue(store, bucketId, buffer.toSlice().toByteArray(), expectedValue, 0, STRING_LIST_SERDE);
  }

  public static <T> void checkValue(SpillableStateStore store, long bucketId, byte[] bytes,
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

    T string = serde.deserialize(new Input(slice.buffer, slice.offset + offset, slice.length));

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
